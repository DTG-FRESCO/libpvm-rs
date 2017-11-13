extern crate nix;
extern crate futures;

use self::nix::sys::socket::{getsockname, getsockopt, SockAddr, SockType, sockopt};
use self::nix::sys::stat::fstat;
use std::os::unix::io::{FromRawFd, RawFd};
use std::os::unix;
use std::error::Error;
use std::fs;
use std::io::Read;
use std::net;
use std::io;

pub struct UdpSocketR(pub net::UdpSocket);

pub enum IOType {
    File,
    TcpStream,
    UdpSocket,
    UnixStream,
    Unknown(String),
}

pub enum FdClass {
    File,
    Socket,
    Fifo,
    Terminal,
    Other,
}

pub struct IOStream {
    src: Box<Read>,
}

impl Read for UdpSocketR {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.recv(buf)
    }
}

impl FromRawFd for IOStream {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        let iotype = match get_fd_type(fd) {
            Ok(t) => t,
            Err(e) => IOType::Unknown(e),
        };
        let fd_obj = match iotype {
            IOType::File => Box::new(fs::File::from_raw_fd(fd)) as Box<Read>,
            IOType::TcpStream => Box::new(net::TcpStream::from_raw_fd(fd)) as Box<Read>,
            IOType::UdpSocket => Box::new(UdpSocketR(net::UdpSocket::from_raw_fd(fd))) as Box<Read>,
            IOType::UnixStream => Box::new(unix::net::UnixStream::from_raw_fd(fd)) as Box<Read>,
            IOType::Unknown(e) => {
                panic!(
                    "Unsupported input stream. You have passed a fd type that is not supported by libopus: {}",
                    e
                )
            }
        };
        IOStream { src: fd_obj }
    }
}

impl Read for IOStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (*self.src).read(buf)
    }
}


const S_IFMT: u32 = 0o170000;
const S_IFSOCK: u32 = 0o140000;
/*const S_IFLNK : u32 = 0o120000;*/
const S_IFREG: u32 = 0o100000;
/*const S_IFBLK : u32 = 0o60000;*/
/*const S_IFDIR : u32 = 0o40000;*/
const S_IFCHR: u32 = 0o20000;
const S_IFIFO: u32 = 0o10000;

fn err_str(err: self::nix::Error) -> String {
    err.description().to_owned()
}

fn get_fd_type(fd: RawFd) -> Result<IOType, String> {
    let fs = fstat(fd).map_err(err_str)?;
    println!("Mode: {}, masked: {}", fs.st_mode, fs.st_mode & S_IFMT);
    let class = match fs.st_mode & S_IFMT {
        S_IFREG => FdClass::File,
        S_IFSOCK => FdClass::Socket,
        S_IFIFO => FdClass::Fifo,
        S_IFCHR => FdClass::Terminal,
        _ => FdClass::Other,
    };
    match class {
        FdClass::File => Ok(IOType::File),
        FdClass::Socket => {
            let saddr = getsockname(fd).map_err(err_str)?;
            match saddr {
                SockAddr::Inet(_) => {
                    let stype = getsockopt(fd, sockopt::SockType).map_err(err_str)?;
                    match stype {
                        SockType::Stream => Ok(IOType::TcpStream),
                        SockType::Datagram => Ok(IOType::UdpSocket),
                        _ => Err(String::from("unsupported socket type")),
                    }
                }
                SockAddr::Unix(_) => Ok(IOType::UnixStream),
                _ => Err(String::from("unsupported socket family")),
            }
        }
        FdClass::Fifo => Err(String::from("fifo input not supported")),
        FdClass::Terminal => Err(String::from("stdin input not supported")),
        _ => Err(String::from("unknown fd type")),
    }
}
