//! CADETS trace format PVM mapping
//!
//! This module contains the definition of the PVM mapping for the CADETS trace format.

use std::fmt;

use crate::{
    data::{
        node_types::{ConcreteType, ContextType, Name, PVMDataType::*},
        ID,
    },
    ingest::{
        pvm::{ConnectDir, PVMError, PVMResult, PVMTransaction, PVM},
        Mapped,
    },
    trace::MapFmt,
};

use chrono::{serde::ts_nanoseconds, DateTime, Utc};
use lazy_static::lazy_static;
use maplit::hashmap;
use serde_derive::Deserialize;
use uuid::Uuid;

lazy_static! {
    static ref PROCESS: ConcreteType = ConcreteType {
        pvm_ty: Actor,
        name: "process",
        props: hashmap!("euid" => true,
                        "ruid" => true,
                        "suid" => true,
                        "egid" => true,
                        "rgid" => true,
                        "sgid" => true,
                        "pid" => false,
                        "cmdline" => true,
                        "login_name" => true),
    };
    static ref FILE: ConcreteType = ConcreteType {
        pvm_ty: Store,
        name: "file",
        props: hashmap!("owner_uid" => true,
                        "owner_gid" => true,
                        "mode" => true),
    };
    static ref SOCKET: ConcreteType = ConcreteType {
        pvm_ty: Conduit,
        name: "socket",
        props: hashmap!(),
    };
    static ref PIPE: ConcreteType = ConcreteType {
        pvm_ty: Conduit,
        name: "pipe",
        props: hashmap!(),
    };
    static ref PTTY: ConcreteType = ConcreteType {
        pvm_ty: Conduit,
        name: "ptty",
        props: hashmap!("owner_uid" => true,
                        "owner_gid" => true,
                        "mode" => true),
    };
    static ref CTX: ContextType = ContextType {
        name: "cadets_context",
        props: vec!["time", "event", "host", "trace_offset"],
    };
}

/// An Audit event
#[derive(Deserialize, Debug)]
pub struct AuditEvent {
    pub offset: Option<usize>,
    pub event: String,
    #[serde(with = "ts_nanoseconds")]
    pub time: DateTime<Utc>,
    pub pid: i32,
    pub ppid: i32,
    pub tid: i32,
    pub uid: i32,
    pub exec: String,
    pub retval: i32,
    pub subjprocuuid: Uuid,
    pub subjthruuid: Uuid,
    pub host: Option<Uuid>,
    pub fd: Option<i32>,
    pub cpu_id: Option<i32>,
    pub cmdline: Option<String>,
    pub upath1: Option<String>,
    pub upath2: Option<String>,
    pub flags: Option<i32>,
    pub fdpath: Option<String>,
    pub arg_objuuid1: Option<Uuid>,
    pub arg_objuuid2: Option<Uuid>,
    pub ret_objuuid1: Option<Uuid>,
    pub ret_objuuid2: Option<Uuid>,
    pub ret_fd1: Option<i32>,
    pub ret_fd2: Option<i32>,
    pub arg_mem_flags: Option<Vec<String>>,
    pub arg_sharing_flags: Option<Vec<String>>,
    pub address: Option<String>,
    pub port: Option<u16>,
    pub arg_uid: Option<i64>,
    pub arg_euid: Option<i64>,
    pub arg_ruid: Option<i64>,
    pub arg_suid: Option<i64>,
    pub arg_gid: Option<i64>,
    pub arg_egid: Option<i64>,
    pub arg_rgid: Option<i64>,
    pub arg_sgid: Option<i64>,
    pub login: Option<String>,
    pub mode: Option<u32>,
}

impl fmt::Display for AuditEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut ret = f.debug_map();
        fields_to_map!(
            ret;
            self.event,
            self.time,
            self.pid,
            self.ppid,
            self.tid,
            self.uid,
            self.exec,
            self.retval,
            self.subjprocuuid,
            self.subjthruuid,
            self.host,
            self.cpu_id,
            self.cmdline,
            self.upath1,
            self.upath2,
            self.fd,
            self.flags,
            self.fdpath,
            self.arg_objuuid1,
            self.arg_objuuid2,
            self.ret_objuuid1,
            self.ret_objuuid2,
            self.ret_fd1,
            self.ret_fd2,
            self.arg_mem_flags,
            self.arg_sharing_flags,
            self.address,
            self.port,
            self.arg_uid,
            self.arg_euid,
            self.arg_ruid,
            self.arg_suid,
            self.arg_gid,
            self.arg_egid,
            self.arg_rgid,
            self.arg_sgid,
            self.login,
            self.mode,
        );
        ret.finish()
    }
}

impl AuditEvent {
    fn opt_sock_name(&self) -> PVMResult<Option<Name>> {
        Ok(if let Some(pth) = self.upath1.clone() {
            Some(Name::Path(pth))
        } else if let Some(prt) = self.port {
            let addr = field!(self.address);
            Some(Name::Net(addr, prt))
        } else {
            None
        })
    }

    fn sock_name(&self) -> PVMResult<Name> {
        if let Some(n) = self.opt_sock_name()? {
            Ok(n)
        } else {
            Err(PVMError::MissingField {
                evt: self.event.clone(),
                field: "upath1, port",
            })
        }
    }

    fn posix_exec(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let cmdline = field!(&self.cmdline);
        let binuuid = field!(self.arg_objuuid1);
        let binname = field!(self.upath1);

        let bin = pvm.declare(&FILE, binuuid, None)?;
        pvm.name(bin, Name::Path(binname))?;

        pvm.meta(pro, "cmdline", cmdline)?;
        pvm.source(pro, bin)?;

        if let Some(lduuid) = self.arg_objuuid2 {
            let ldname = field!(self.upath2);

            let ld = pvm.declare(&FILE, lduuid, None)?;
            pvm.name(ld, Name::Path(ldname))?;

            pvm.source(pro, ld)?;
        }

        Ok(())
    }

    fn posix_fork(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let ret_objuuid1 = field!(self.ret_objuuid1);

        let ch = pvm.derive(pro, ret_objuuid1)?;

        pvm.meta(ch, "pid", &self.retval)?;
        pvm.source(ch, pro)?;
        Ok(())
    }

    fn posix_exit(&self, _pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        pvm.release(&self.subjprocuuid);
        Ok(())
    }

    fn posix_open(&self, _pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        if let Some(fuuid) = self.ret_objuuid1 {
            let fname = field!(self.upath1);

            let f = pvm.declare(&FILE, fuuid, None)?;
            pvm.name(f, Name::Path(fname))?;
        }
        Ok(())
    }

    fn posix_read(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let fuuid = field!(self.arg_objuuid1);

        let f = pvm.declare(&FILE, fuuid, None)?;
        if let Some(pth) = self.fdpath.clone() {
            if pth != "<unknown>" {
                pvm.name(f, Name::Path(pth))?;
            }
        }
        pvm.source_nbytes(pro, f, self.retval)?;
        Ok(())
    }

    fn posix_write(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let fuuid = field!(self.arg_objuuid1);

        let f = pvm.declare(&FILE, fuuid, None)?;
        if let Some(pth) = self.fdpath.clone() {
            if pth != "<unknown>" {
                pvm.name(f, Name::Path(pth))?;
            }
        }
        pvm.sinkstart_nbytes(pro, f, self.retval)?;
        Ok(())
    }

    fn posix_close(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        if let Some(fuuid) = self.arg_objuuid1 {
            let f = pvm.declare(&FILE, fuuid, None)?;
            pvm.sinkend(pro, f)?;
        }
        Ok(())
    }

    fn posix_socket(&self, _pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let suuid = field!(self.ret_objuuid1);
        pvm.declare(&SOCKET, suuid, None)?;
        Ok(())
    }

    fn posix_listen(&self, _pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let suuid = field!(self.arg_objuuid1);
        pvm.declare(&SOCKET, suuid, None)?;
        Ok(())
    }

    fn posix_bind(&self, _pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let suuid = field!(self.arg_objuuid1);
        let s = pvm.declare(&SOCKET, suuid, None)?;
        pvm.name(s, self.sock_name()?)?;
        Ok(())
    }

    fn posix_accept(&self, _pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let luuid = field!(self.arg_objuuid1);
        let ruuid = field!(self.ret_objuuid1);
        pvm.declare(&SOCKET, luuid, None)?;
        let r = pvm.declare(&SOCKET, ruuid, None)?;
        pvm.name(r, self.sock_name()?)?;
        Ok(())
    }

    fn posix_connect(&self, _pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let suuid = field!(self.arg_objuuid1);
        let s = pvm.declare(&SOCKET, suuid, None)?;
        pvm.name(s, self.sock_name()?)?;
        Ok(())
    }

    fn posix_mmap(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let fuuid = field!(self.arg_objuuid1);
        let mut f = pvm.declare(&FILE, fuuid, None)?;
        if let Some(fdpath) = self.fdpath.clone() {
            pvm.name(f, Name::Path(fdpath))?;
        }
        if let Some(ref flags) = self.arg_mem_flags {
            if flags.contains(&String::from("PROT_WRITE")) {
                if let Some(ref share_flags) = self.arg_sharing_flags {
                    if !share_flags.contains(&String::from("MAP_PRIVATE")) {
                        pvm.sinkstart(pro, f)?;
                        f = pvm.declare(&FILE, fuuid, None)?;
                    }
                } else {
                    pvm.sinkstart(pro, f)?;
                    f = pvm.declare(&FILE, fuuid, None)?;
                }
            }

            if flags.contains(&String::from("PROT_READ")) {
                pvm.source(pro, f)?;
            }
        }
        Ok(())
    }

    fn posix_socketpair(&self, _pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let ruuid1 = field!(self.ret_objuuid1);
        let ruuid2 = field!(self.ret_objuuid2);
        let s1 = pvm.declare(&SOCKET, ruuid1, None)?;
        let s2 = pvm.declare(&SOCKET, ruuid2, None)?;
        pvm.connect(s1, s2, ConnectDir::BiDirectional)?;
        Ok(())
    }

    fn posix_pipe(&self, _pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let ruuid1 = field!(self.ret_objuuid1);
        let ruuid2 = field!(self.ret_objuuid2);
        let p1 = pvm.declare(&PIPE, ruuid1, None)?;
        let p2 = pvm.declare(&PIPE, ruuid2, None)?;
        pvm.connect(p1, p2, ConnectDir::BiDirectional)?;
        Ok(())
    }

    fn posix_sendmsg(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let suuid = field!(self.arg_objuuid1);
        let s = pvm.declare(&SOCKET, suuid, None)?;
        if let Some(n) = self.opt_sock_name()? {
            pvm.name(s, n)?;
        }
        pvm.sinkstart_nbytes(pro, s, self.retval)?;
        Ok(())
    }

    fn posix_sendto(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let suuid = field!(self.arg_objuuid1);
        let s = pvm.declare(&SOCKET, suuid, None)?;
        if let Some(n) = self.opt_sock_name()? {
            pvm.name(s, n)?;
        }
        pvm.sinkstart_nbytes(pro, s, self.retval)?;
        Ok(())
    }

    fn posix_recvmsg(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let suuid = field!(self.arg_objuuid1);
        let s = pvm.declare(&SOCKET, suuid, None)?;
        if let Some(n) = self.opt_sock_name()? {
            pvm.name(s, n)?;
        }
        pvm.source_nbytes(pro, s, self.retval)?;
        Ok(())
    }

    fn posix_recvfrom(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let suuid = field!(self.arg_objuuid1);
        let s = pvm.declare(&SOCKET, suuid, None)?;
        if let Some(n) = self.opt_sock_name()? {
            pvm.name(s, n)?;
        }
        pvm.source_nbytes(pro, s, self.retval)?;
        Ok(())
    }

    fn posix_chdir(&self, _pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let duuid = field!(self.arg_objuuid1);
        let d = pvm.declare(&FILE, duuid, None)?;
        if let Some(dpath) = self.upath1.clone() {
            pvm.name(d, Name::Path(dpath))?;
        }
        Ok(())
    }

    fn posix_chmod(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let fuuid = field!(self.arg_objuuid1);
        let fpath = field!(self.upath1);
        let mode = field!(self.mode);
        let f = pvm.declare(&FILE, fuuid, None)?;
        pvm.meta(f, "mode", &format!("{:o}", mode))?;
        pvm.name(f, Name::Path(fpath))?;
        pvm.sink(pro, f)?;
        Ok(())
    }

    fn posix_chown(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let fuuid = field!(self.arg_objuuid1);
        let fpath = field!(self.upath1);
        let arg_uid = field!(self.arg_uid);
        let arg_gid = field!(self.arg_gid);
        let f = pvm.declare(&FILE, fuuid, None)?;
        pvm.meta(f, "owner_uid", &arg_uid)?;
        pvm.meta(f, "owner_gid", &arg_gid)?;
        pvm.name(f, Name::Path(fpath))?;
        pvm.sink(pro, f)?;
        Ok(())
    }

    fn posix_fchmod(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let fuuid = field!(self.arg_objuuid1);
        let mode = field!(self.mode);
        let f = pvm.declare(&FILE, fuuid, None)?;
        pvm.meta(f, "mode", &format!("{:o}", mode))?;
        pvm.sinkstart(pro, f)?;
        Ok(())
    }

    fn posix_fchown(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let fuuid = field!(self.arg_objuuid1);
        let arg_uid = field!(self.arg_uid);
        let arg_gid = field!(self.arg_gid);
        let f = pvm.declare(&FILE, fuuid, None)?;
        pvm.meta(f, "owner_uid", &arg_uid)?;
        pvm.meta(f, "owner_gid", &arg_gid)?;
        pvm.sinkstart(pro, f)?;
        Ok(())
    }

    fn posix_posix_openpt(&self, _pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let ttyuuid = field!(self.ret_objuuid1);
        pvm.declare(&PTTY, ttyuuid, None)?;
        Ok(())
    }

    fn posix_link(&self, _pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let fuuid = field!(self.arg_objuuid1);
        let upath1 = field!(self.upath1);
        let upath2 = field!(self.upath2);
        let f = pvm.declare(&FILE, fuuid, None)?;
        pvm.name(f, Name::Path(upath1))?;
        pvm.name(f, Name::Path(upath2))?;
        Ok(())
    }

    fn posix_rename(&self, _pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let src_uuid = field!(self.arg_objuuid1);
        let src = field!(self.upath1);
        let dst = field!(self.upath2);
        let fsrc = pvm.declare(&FILE, src_uuid, None)?;
        pvm.unname(fsrc, Name::Path(src))?;
        if let Some(ovr_uuid) = self.arg_objuuid2 {
            let fovr = pvm.declare(&FILE, ovr_uuid, None)?;
            pvm.unname(fovr, Name::Path(dst.clone()))?;
        }
        pvm.name(fsrc, Name::Path(dst))?;
        Ok(())
    }

    fn posix_unlink(&self, _pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let fuuid = field!(self.arg_objuuid1);
        let upath1 = field!(self.upath1);
        let f = pvm.declare(&FILE, fuuid, None)?;
        pvm.unname(f, Name::Path(upath1))?;
        Ok(())
    }

    fn posix_setuid(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let uid = field!(&self.arg_uid);
        pvm.meta(pro, "euid", uid)?;
        pvm.meta(pro, "ruid", uid)?;
        pvm.meta(pro, "suid", uid)?;
        Ok(())
    }

    fn posix_seteuid(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let euid = field!(&self.arg_euid);
        pvm.meta(pro, "euid", euid)?;
        Ok(())
    }

    fn posix_setreuid(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let ruid = field!(&self.arg_ruid);
        let euid = field!(&self.arg_euid);
        if *ruid != -1 {
            pvm.meta(pro, "ruid", ruid)?;
        }
        if *euid != -1 {
            pvm.meta(pro, "euid", euid)?;
        }
        Ok(())
    }

    fn posix_setresuid(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let ruid = field!(&self.arg_ruid);
        let euid = field!(&self.arg_euid);
        let suid = field!(&self.arg_suid);
        if *ruid != -1 {
            pvm.meta(pro, "ruid", ruid)?;
        }
        if *euid != -1 {
            pvm.meta(pro, "euid", euid)?;
        }
        if *suid != -1 {
            pvm.meta(pro, "suid", suid)?;
        }
        Ok(())
    }

    fn posix_setgid(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let gid = field!(&self.arg_gid);
        pvm.meta(pro, "egid", gid)?;
        pvm.meta(pro, "rgid", gid)?;
        pvm.meta(pro, "sgid", gid)?;
        Ok(())
    }

    fn posix_setegid(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let egid = field!(&self.arg_egid);
        pvm.meta(pro, "egid", egid)?;
        Ok(())
    }

    fn posix_setregid(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let rgid = field!(&self.arg_rgid);
        let egid = field!(&self.arg_egid);
        if *rgid != -1 {
            pvm.meta(pro, "rgid", rgid)?;
        }
        if *egid != -1 {
            pvm.meta(pro, "egid", egid)?;
        }
        Ok(())
    }

    fn posix_setresgid(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let rgid = field!(&self.arg_rgid);
        let egid = field!(&self.arg_egid);
        let sgid = field!(&self.arg_sgid);
        if *rgid != -1 {
            pvm.meta(pro, "rgid", rgid)?;
        }
        if *egid != -1 {
            pvm.meta(pro, "egid", egid)?;
        }
        if *sgid != -1 {
            pvm.meta(pro, "sgid", sgid)?;
        }
        Ok(())
    }

    fn posix_setlogin(&self, pro: ID, pvm: &mut PVMTransaction) -> PVMResult<()> {
        let login = field!(&self.login);
        pvm.meta(pro, "login_name", login)?;
        Ok(())
    }

    fn parse(&self, pvm: &mut PVM) -> PVMResult<()> {
        let mut ctx = hashmap!(
            "event" => self.event.clone(),
            "host" => field!(self.host).to_hyphenated_ref().to_string(),
            "time" => self.time.to_rfc3339(),
        );
        if let Some(offset) = self.offset {
            ctx.insert("trace_offset", offset.to_string());
        }
        let mut tr = pvm.transaction(&CTX, ctx);
        match {
            let pro = tr.declare(
                &PROCESS,
                self.subjprocuuid,
                Some(hashmap!("cmdline" => self.exec.clone(),
                         "pid" => self.pid.to_string())),
            )?;
            match &self.event[..] {
                "audit:event:aue_accept:" => self.posix_accept(pro, &mut tr),
                "audit:event:aue_bind:" => self.posix_bind(pro, &mut tr),
                "audit:event:aue_chdir:" | "audit:event:aue_fchdir:" => {
                    self.posix_chdir(pro, &mut tr)
                }
                "audit:event:aue_chmod:" | "audit:event:aue_fchmodat:" => {
                    self.posix_chmod(pro, &mut tr)
                }
                "audit:event:aue_chown:" => self.posix_chown(pro, &mut tr),
                "audit:event:aue_close:" => self.posix_close(pro, &mut tr),
                "audit:event:aue_connect:" => self.posix_connect(pro, &mut tr),
                "audit:event:aue_execve:" => self.posix_exec(pro, &mut tr),
                "audit:event:aue_exit:" => self.posix_exit(pro, &mut tr),
                "audit:event:aue_fork:" | "audit:event:aue_pdfork:" | "audit:event:aue_vfork:" => {
                    self.posix_fork(pro, &mut tr)
                }
                "audit:event:aue_fchmod:" => self.posix_fchmod(pro, &mut tr),
                "audit:event:aue_fchown:" => self.posix_fchown(pro, &mut tr),
                "audit:event:aue_link:" => self.posix_link(pro, &mut tr),
                "audit:event:aue_listen:" => self.posix_listen(pro, &mut tr),
                "audit:event:aue_mmap:" => self.posix_mmap(pro, &mut tr),
                "audit:event:aue_open_rwtc:" | "audit:event:aue_openat_rwtc:" => {
                    self.posix_open(pro, &mut tr)
                }
                "audit:event:aue_pipe:" => self.posix_pipe(pro, &mut tr),
                "audit:event:aue_posix_openpt:" => self.posix_posix_openpt(pro, &mut tr),
                "audit:event:aue_read:" | "audit:event:aue_pread:" => self.posix_read(pro, &mut tr),
                "audit:event:aue_recvmsg:" => self.posix_recvmsg(pro, &mut tr),
                "audit:event:aue_recvfrom:" => self.posix_recvfrom(pro, &mut tr),
                "audit:event:aue_rename:" => self.posix_rename(pro, &mut tr),
                "audit:event:aue_sendmsg:" => self.posix_sendmsg(pro, &mut tr),
                "audit:event:aue_sendto:" => self.posix_sendto(pro, &mut tr),
                "audit:event:aue_setegid:" => self.posix_setegid(pro, &mut tr),
                "audit:event:aue_seteuid:" => self.posix_seteuid(pro, &mut tr),
                "audit:event:aue_setlogin:" => self.posix_setlogin(pro, &mut tr),
                "audit:event:aue_setgid:" => self.posix_setgid(pro, &mut tr),
                "audit:event:aue_setregid:" => self.posix_setregid(pro, &mut tr),
                "audit:event:aue_setresgid:" => self.posix_setresgid(pro, &mut tr),
                "audit:event:aue_setresuid:" => self.posix_setresuid(pro, &mut tr),
                "audit:event:aue_setreuid:" => self.posix_setreuid(pro, &mut tr),
                "audit:event:aue_setuid:" => self.posix_setuid(pro, &mut tr),
                "audit:event:aue_socket:" => self.posix_socket(pro, &mut tr),
                "audit:event:aue_socketpair:" => self.posix_socketpair(pro, &mut tr),
                "audit:event:aue_unlink:" => self.posix_unlink(pro, &mut tr),
                "audit:event:aue_write:"
                | "audit:event:aue_pwrite:"
                | "audit:event:aue_writev:" => self.posix_write(pro, &mut tr),
                "audit:event:aue_dup2:" => Ok(()), /* IGNORE */
                _ => {
                    //tr.unparsed_events.insert(self.event.clone());
                    Ok(())
                }
            }
        } {
            Ok(_) => {
                tr.commit();
                Ok(())
            }
            Err(e) => {
                tr.rollback();
                Err(e)
            }
        }
    }
}

/// A FBT type event
#[derive(Deserialize, Debug)]
pub struct FBTEvent {
    pub offset: Option<usize>,
    pub event: String,
    pub host: Uuid,
    #[serde(with = "ts_nanoseconds")]
    pub time: DateTime<Utc>,
    pub so_uuid: Uuid,
    pub lport: i32,
    pub fport: i32,
    pub laddr: String,
    pub faddr: String,
}

impl fmt::Display for FBTEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut ret = f.debug_map();
        fields_to_map!(
            ret;
            self.event,
            self.host,
            self.time,
            self.so_uuid,
            self.lport,
            self.fport,
            self.laddr,
            self.faddr
        );
        ret.finish()
    }
}

/// A CADETS trace event
#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum TraceEvent {
    Audit(Box<AuditEvent>),
    FBT(FBTEvent),
}

impl fmt::Display for TraceEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TraceEvent::Audit(box ae) => {
                write!(f, "TraceEvent::Audit(")?;
                <AuditEvent as fmt::Display>::fmt(ae, f)?;
                write!(f, ")")
            }
            TraceEvent::FBT(fbt) => {
                write!(f, "TraceEvent::FBT(")?;
                <FBTEvent as fmt::Display>::fmt(fbt, f)?;
                write!(f, ")")
            }
        }
    }
}

impl Mapped for TraceEvent {
    fn init(pvm: &mut PVM) {
        pvm.register_data_type(&PROCESS);
        pvm.register_data_type(&FILE);
        pvm.register_data_type(&SOCKET);
        pvm.register_data_type(&PIPE);
        pvm.register_data_type(&PTTY);
        pvm.register_ctx_type(&CTX);
    }

    fn update(&mut self) {
        if let TraceEvent::Audit(e) = self {
            if let Some(host) = e.host {
                let map_uuid = |u: Uuid| Uuid::new_v5(&host, u.as_bytes());

                e.arg_objuuid1 = e.arg_objuuid1.map(map_uuid);
                e.arg_objuuid2 = e.arg_objuuid2.map(map_uuid);
                e.ret_objuuid1 = e.ret_objuuid1.map(map_uuid);
                e.ret_objuuid2 = e.ret_objuuid2.map(map_uuid);
                e.subjprocuuid = map_uuid(e.subjprocuuid);
                e.subjthruuid = map_uuid(e.subjthruuid);
            }
        }
    }

    fn process(&self, pvm: &mut PVM) -> PVMResult<()> {
        match self {
            TraceEvent::Audit(box tr) => tr.parse(pvm),
            TraceEvent::FBT(_) => Ok(()),
        }
    }

    fn set_offset(&mut self, offset: usize) {
        match self {
            TraceEvent::Audit(e) => {
                e.offset = Some(offset);
            }
            TraceEvent::FBT(e) => {
                e.offset = Some(offset);
            }
        }
    }
}
