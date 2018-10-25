use chrono::{serde::ts_nanoseconds, DateTime, Utc};
use std::fmt;
use uuid::Uuid;

use data::node_types::{ConcreteType, ContextType, Name, PVMDataType::*};

use ingest::{
    pvm::{ConnectDir, NodeGuard, PVMError, PVM},
    Parseable,
};

use super::MapFmt;

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
        props: vec!["time", "event", "host"],
    };
}

#[derive(Deserialize, Debug)]
pub struct AuditEvent {
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
    fn opt_sock_name(&self) -> Result<Option<Name>, PVMError> {
        Ok(if let Some(pth) = self.upath1.clone() {
            Some(Name::Path(pth))
        } else if let Some(prt) = self.port {
            let addr = clone_field!(self.address);
            Some(Name::Net(addr, prt))
        } else {
            None
        })
    }

    fn sock_name(&self) -> Result<Name, PVMError> {
        if let Some(n) = self.opt_sock_name()? {
            Ok(n)
        } else {
            Err(PVMError::MissingField {
                evt: self.event.clone(),
                field: "upath1, port",
            })
        }
    }

    fn posix_exec(&self, pro: &mut NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let cmdline = ref_field!(self.cmdline);
        let binuuid = field!(self.arg_objuuid1);
        let binname = clone_field!(self.upath1);
        let lduuid = field!(self.arg_objuuid2);
        let ldname = clone_field!(self.upath2);

        let bin = pvm.declare(&FILE, binuuid, None);
        pvm.name(&bin, Name::Path(binname));

        let ld = pvm.declare(&FILE, lduuid, None);
        pvm.name(&ld, Name::Path(ldname));

        pvm.meta(pro, "cmdline", cmdline);
        pvm.source(pro, &bin);
        pvm.source(pro, &ld);

        Ok(())
    }

    fn posix_fork(&self, pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let ret_objuuid1 = field!(self.ret_objuuid1);

        let mut ch = pvm.declare(&PROCESS, ret_objuuid1, None);

        ch.meta.merge(&pro.meta.snapshot(pvm.ctx()));
        pvm.meta(&mut ch, "pid", &self.retval);
        pvm.source(&ch, pro);
        Ok(())
    }

    fn posix_exit(&self, _pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        pvm.release(&self.subjprocuuid);
        Ok(())
    }

    fn posix_open(&self, _pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        if let Some(fuuid) = self.ret_objuuid1 {
            let fname = clone_field!(self.upath1);

            let f = pvm.declare(&FILE, fuuid, None);
            pvm.name(&f, Name::Path(fname));
        }
        Ok(())
    }

    fn posix_read(&self, pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let fuuid = field!(self.arg_objuuid1);

        let f = pvm.declare(&FILE, fuuid, None);
        if let Some(pth) = self.fdpath.clone() {
            if pth != "<unknown>" {
                pvm.name(&f, Name::Path(pth));
            }
        }
        pvm.source_nbytes(pro, &f, self.retval);
        Ok(())
    }

    fn posix_write(&self, pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let fuuid = field!(self.arg_objuuid1);

        let f = pvm.declare(&FILE, fuuid, None);
        if let Some(pth) = self.fdpath.clone() {
            if pth != "<unknown>" {
                pvm.name(&f, Name::Path(pth));
            }
        }
        pvm.sinkstart_nbytes(pro, &f, self.retval);
        Ok(())
    }

    fn posix_close(&self, pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        if let Some(fuuid) = self.arg_objuuid1 {
            let f = pvm.declare(&FILE, fuuid, None);
            pvm.sinkend(pro, &f);
        }
        Ok(())
    }

    fn posix_socket(&self, _pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let suuid = field!(self.ret_objuuid1);
        pvm.declare(&SOCKET, suuid, None);
        Ok(())
    }

    fn posix_listen(&self, _pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let suuid = field!(self.arg_objuuid1);
        pvm.declare(&SOCKET, suuid, None);
        Ok(())
    }

    fn posix_bind(&self, _pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let suuid = field!(self.arg_objuuid1);
        let s = pvm.declare(&SOCKET, suuid, None);
        pvm.name(&s, self.sock_name()?);
        Ok(())
    }

    fn posix_accept(&self, _pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let luuid = field!(self.arg_objuuid1);
        let ruuid = field!(self.ret_objuuid1);
        pvm.declare(&SOCKET, luuid, None);
        let r = pvm.declare(&SOCKET, ruuid, None);
        pvm.name(&r, self.sock_name()?);
        Ok(())
    }

    fn posix_connect(&self, _pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let suuid = field!(self.arg_objuuid1);
        let s = pvm.declare(&SOCKET, suuid, None);
        pvm.name(&s, self.sock_name()?);
        Ok(())
    }

    fn posix_mmap(&self, pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let fuuid = field!(self.arg_objuuid1);
        let f = pvm.declare(&FILE, fuuid, None);
        if let Some(fdpath) = self.fdpath.clone() {
            pvm.name(&f, Name::Path(fdpath));
        }
        if let Some(flags) = self.arg_mem_flags.clone() {
            if flags.contains(&String::from("PROT_WRITE")) {
                if let Some(share_flags) = self.arg_sharing_flags.clone() {
                    if !share_flags.contains(&String::from("MAP_PRIVATE")) {
                        pvm.sinkstart(pro, &f);
                    }
                } else {
                    pvm.sinkstart(pro, &f);
                }
            }

            if flags.contains(&String::from("PROT_READ")) {
                pvm.source(pro, &f);
            }
        }
        Ok(())
    }

    fn posix_socketpair(&self, _pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let ruuid1 = field!(self.ret_objuuid1);
        let ruuid2 = field!(self.ret_objuuid2);
        let s1 = pvm.declare(&SOCKET, ruuid1, None);
        let s2 = pvm.declare(&SOCKET, ruuid2, None);
        pvm.connect(&s1, &s2, ConnectDir::BiDirectional);
        Ok(())
    }

    fn posix_pipe(&self, _pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let ruuid1 = field!(self.ret_objuuid1);
        let ruuid2 = field!(self.ret_objuuid2);
        let p1 = pvm.declare(&PIPE, ruuid1, None);
        let p2 = pvm.declare(&PIPE, ruuid2, None);
        pvm.connect(&p1, &p2, ConnectDir::BiDirectional);
        Ok(())
    }

    fn posix_sendmsg(&self, pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let suuid = field!(self.arg_objuuid1);
        let s = pvm.declare(&SOCKET, suuid, None);
        if let Some(n) = self.opt_sock_name()? {
            pvm.name(&s, n);
        }
        pvm.sinkstart_nbytes(pro, &s, self.retval);
        Ok(())
    }

    fn posix_sendto(&self, pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let suuid = field!(self.arg_objuuid1);
        let s = pvm.declare(&SOCKET, suuid, None);
        if let Some(n) = self.opt_sock_name()? {
            pvm.name(&s, n);
        }
        pvm.sinkstart_nbytes(pro, &s, self.retval);
        Ok(())
    }

    fn posix_recvmsg(&self, pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let suuid = field!(self.arg_objuuid1);
        let s = pvm.declare(&SOCKET, suuid, None);
        if let Some(n) = self.opt_sock_name()? {
            pvm.name(&s, n);
        }
        pvm.source_nbytes(pro, &s, self.retval);
        Ok(())
    }

    fn posix_recvfrom(&self, pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let suuid = field!(self.arg_objuuid1);
        let s = pvm.declare(&SOCKET, suuid, None);
        if let Some(n) = self.opt_sock_name()? {
            pvm.name(&s, n);
        }
        pvm.source_nbytes(pro, &s, self.retval);
        Ok(())
    }

    fn posix_chdir(&self, _pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let duuid = field!(self.arg_objuuid1);
        let d = pvm.declare(&FILE, duuid, None);
        if let Some(dpath) = self.upath1.clone() {
            pvm.name(&d, Name::Path(dpath));
        }
        Ok(())
    }

    fn posix_chmod(&self, pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let fuuid = field!(self.arg_objuuid1);
        let fpath = clone_field!(self.upath1);
        let mode = field!(self.mode);
        let mut f = pvm.declare(&FILE, fuuid, None);
        pvm.meta(&mut f, "mode", &format!("{:o}", mode));
        pvm.name(&f, Name::Path(fpath));
        pvm.sink(pro, &f);
        Ok(())
    }

    fn posix_chown(&self, pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let fuuid = field!(self.arg_objuuid1);
        let fpath = clone_field!(self.upath1);
        let arg_uid = field!(self.arg_uid);
        let arg_gid = field!(self.arg_gid);
        let mut f = pvm.declare(&FILE, fuuid, None);
        pvm.meta(&mut f, "owner_uid", &arg_uid);
        pvm.meta(&mut f, "owner_gid", &arg_gid);
        pvm.name(&f, Name::Path(fpath));
        pvm.sink(pro, &f);
        Ok(())
    }

    fn posix_fchmod(&self, pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let fuuid = field!(self.arg_objuuid1);
        let mode = field!(self.mode);
        let mut f = pvm.declare(&FILE, fuuid, None);
        pvm.meta(&mut f, "mode", &format!("{:o}", mode));
        pvm.sinkstart(pro, &f);
        Ok(())
    }

    fn posix_fchown(&self, pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let fuuid = field!(self.arg_objuuid1);
        let arg_uid = field!(self.arg_uid);
        let arg_gid = field!(self.arg_gid);
        let mut f = pvm.declare(&FILE, fuuid, None);
        pvm.meta(&mut f, "owner_uid", &arg_uid);
        pvm.meta(&mut f, "owner_gid", &arg_gid);
        pvm.sinkstart(pro, &f);
        Ok(())
    }

    fn posix_posix_openpt(&self, _pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let ttyuuid = field!(self.ret_objuuid1);
        pvm.declare(&PTTY, ttyuuid, None);
        Ok(())
    }

    fn posix_link(&self, _pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let fuuid = field!(self.arg_objuuid1);
        let upath1 = clone_field!(self.upath1);
        let upath2 = clone_field!(self.upath2);
        let f = pvm.declare(&FILE, fuuid, None);
        pvm.name(&f, Name::Path(upath1));
        pvm.name(&f, Name::Path(upath2));
        Ok(())
    }

    fn posix_rename(&self, _pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let src_uuid = field!(self.arg_objuuid1);
        let src = clone_field!(self.upath1);
        let dst = clone_field!(self.upath2);
        let fsrc = pvm.declare(&FILE, src_uuid, None);
        pvm.unname(&fsrc, Name::Path(src));
        if let Some(ovr_uuid) = self.arg_objuuid2 {
            let fovr = pvm.declare(&FILE, ovr_uuid, None);
            pvm.unname(&fovr, Name::Path(dst.clone()));
        }
        pvm.name(&fsrc, Name::Path(dst));
        Ok(())
    }

    fn posix_unlink(&self, _pro: &NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let fuuid = field!(self.arg_objuuid1);
        let upath1 = clone_field!(self.upath1);
        let f = pvm.declare(&FILE, fuuid, None);
        pvm.unname(&f, Name::Path(upath1));
        Ok(())
    }

    fn posix_setuid(&self, pro: &mut NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let uid = ref_field!(self.arg_uid);
        pvm.meta(pro, "euid", uid);
        pvm.meta(pro, "ruid", uid);
        pvm.meta(pro, "suid", uid);
        Ok(())
    }

    fn posix_seteuid(&self, pro: &mut NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let euid = ref_field!(self.arg_euid);
        pvm.meta(pro, "euid", euid);
        Ok(())
    }

    fn posix_setreuid(&self, pro: &mut NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let ruid = ref_field!(self.arg_ruid);
        let euid = ref_field!(self.arg_euid);
        if *ruid != -1 {
            pvm.meta(pro, "ruid", ruid);
        }
        if *euid != -1 {
            pvm.meta(pro, "euid", euid);
        }
        Ok(())
    }

    fn posix_setresuid(&self, pro: &mut NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let ruid = ref_field!(self.arg_ruid);
        let euid = ref_field!(self.arg_euid);
        let suid = ref_field!(self.arg_suid);
        if *ruid != -1 {
            pvm.meta(pro, "ruid", ruid);
        }
        if *euid != -1 {
            pvm.meta(pro, "euid", euid);
        }
        if *suid != -1 {
            pvm.meta(pro, "suid", suid);
        }
        Ok(())
    }

    fn posix_setgid(&self, pro: &mut NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let gid = ref_field!(self.arg_gid);
        pvm.meta(pro, "egid", gid);
        pvm.meta(pro, "rgid", gid);
        pvm.meta(pro, "sgid", gid);
        Ok(())
    }

    fn posix_setegid(&self, pro: &mut NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let egid = ref_field!(self.arg_egid);
        pvm.meta(pro, "egid", egid);
        Ok(())
    }

    fn posix_setregid(&self, pro: &mut NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let rgid = ref_field!(self.arg_rgid);
        let egid = ref_field!(self.arg_egid);
        if *rgid != -1 {
            pvm.meta(pro, "rgid", rgid);
        }
        if *egid != -1 {
            pvm.meta(pro, "egid", egid);
        }
        Ok(())
    }

    fn posix_setresgid(&self, pro: &mut NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let rgid = ref_field!(self.arg_rgid);
        let egid = ref_field!(self.arg_egid);
        let sgid = ref_field!(self.arg_sgid);
        if *rgid != -1 {
            pvm.meta(pro, "rgid", rgid);
        }
        if *egid != -1 {
            pvm.meta(pro, "egid", egid);
        }
        if *sgid != -1 {
            pvm.meta(pro, "sgid", sgid);
        }
        Ok(())
    }

    fn posix_setlogin(&self, pro: &mut NodeGuard, pvm: &mut PVM) -> Result<(), PVMError> {
        let login = ref_field!(self.login);
        pvm.meta(pro, "login_name", login);
        Ok(())
    }

    fn parse(&self, pvm: &mut PVM) -> Result<(), PVMError> {
        pvm.new_ctx(
            &CTX,
            hashmap!(
                "event" => self.event.clone(),
                "host" => self.host.unwrap().hyphenated().to_string(),
                "time" => self.time.to_rfc3339(),
            ),
        );
        let mut pro = pvm.declare(
            &PROCESS,
            self.subjprocuuid,
            Some(hashmap!("cmdline" => self.exec.clone(),
                         "pid" => self.pid.to_string())),
        );
        match &self.event[..] {
            "audit:event:aue_accept:" => self.posix_accept(&pro, pvm),
            "audit:event:aue_bind:" => self.posix_bind(&pro, pvm),
            "audit:event:aue_chdir:" | "audit:event:aue_fchdir:" => self.posix_chdir(&pro, pvm),
            "audit:event:aue_chmod:" | "audit:event:aue_fchmodat:" => self.posix_chmod(&pro, pvm),
            "audit:event:aue_chown:" => self.posix_chown(&pro, pvm),
            "audit:event:aue_close:" => self.posix_close(&pro, pvm),
            "audit:event:aue_connect:" => self.posix_connect(&pro, pvm),
            "audit:event:aue_execve:" => self.posix_exec(&mut pro, pvm),
            "audit:event:aue_exit:" => self.posix_exit(&pro, pvm),
            "audit:event:aue_fork:" | "audit:event:aue_pdfork:" | "audit:event:aue_vfork:" => {
                self.posix_fork(&pro, pvm)
            }
            "audit:event:aue_fchmod:" => self.posix_fchmod(&pro, pvm),
            "audit:event:aue_fchown:" => self.posix_fchown(&pro, pvm),
            "audit:event:aue_link:" => self.posix_link(&pro, pvm),
            "audit:event:aue_listen:" => self.posix_listen(&pro, pvm),
            "audit:event:aue_mmap:" => self.posix_mmap(&pro, pvm),
            "audit:event:aue_open_rwtc:" | "audit:event:aue_openat_rwtc:" => {
                self.posix_open(&pro, pvm)
            }
            "audit:event:aue_pipe:" => self.posix_pipe(&pro, pvm),
            "audit:event:aue_posix_openpt:" => self.posix_posix_openpt(&pro, pvm),
            "audit:event:aue_read:" | "audit:event:aue_pread:" => self.posix_read(&pro, pvm),
            "audit:event:aue_recvmsg:" => self.posix_recvmsg(&pro, pvm),
            "audit:event:aue_recvfrom:" => self.posix_recvfrom(&pro, pvm),
            "audit:event:aue_rename:" => self.posix_rename(&pro, pvm),
            "audit:event:aue_sendmsg:" => self.posix_sendmsg(&pro, pvm),
            "audit:event:aue_sendto:" => self.posix_sendto(&pro, pvm),
            "audit:event:aue_setegid:" => self.posix_setegid(&mut pro, pvm),
            "audit:event:aue_seteuid:" => self.posix_seteuid(&mut pro, pvm),
            "audit:event:aue_setlogin:" => self.posix_setlogin(&mut pro, pvm),
            "audit:event:aue_setgid:" => self.posix_setgid(&mut pro, pvm),
            "audit:event:aue_setregid:" => self.posix_setregid(&mut pro, pvm),
            "audit:event:aue_setresgid:" => self.posix_setresgid(&mut pro, pvm),
            "audit:event:aue_setresuid:" => self.posix_setresuid(&mut pro, pvm),
            "audit:event:aue_setreuid:" => self.posix_setreuid(&mut pro, pvm),
            "audit:event:aue_setuid:" => self.posix_setuid(&mut pro, pvm),
            "audit:event:aue_socket:" => self.posix_socket(&pro, pvm),
            "audit:event:aue_socketpair:" => self.posix_socketpair(&pro, pvm),
            "audit:event:aue_unlink:" => self.posix_unlink(&pro, pvm),
            "audit:event:aue_write:" | "audit:event:aue_pwrite:" | "audit:event:aue_writev:" => {
                self.posix_write(&pro, pvm)
            }
            "audit:event:aue_dup2:" => Ok(()), /* IGNORE */
            _ => {
                pvm.unparsed_events.insert(self.event.clone());
                Ok(())
            }
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct FBTEvent {
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

impl Parseable for TraceEvent {
    fn init(pvm: &mut PVM) {
        pvm.register_data_type(&PROCESS);
        pvm.register_data_type(&FILE);
        pvm.register_data_type(&SOCKET);
        pvm.register_data_type(&PIPE);
        pvm.register_data_type(&PTTY);
        pvm.register_ctx_type(&CTX);
    }

    fn parse(&self, pvm: &mut PVM) -> Result<(), PVMError> {
        match self {
            TraceEvent::Audit(box tr) => tr.parse(pvm),
            TraceEvent::FBT(_) => Ok(()),
        }
    }
}
