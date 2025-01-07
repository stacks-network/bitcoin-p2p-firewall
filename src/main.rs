/*
 copyright: (c) 2013-2020 by Blockstack PBC, a public benefit corporation.

 This file is part of Blockstack.

 Blockstack is free software. You may redistribute or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License or
 (at your option) any later version.

 Blockstack is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY, including without the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with Blockstack. If not, see <http://www.gnu.org/licenses/>.
*/

#![allow(unused_macros)]

extern crate mio;

use std::error;
use std::fmt;
use std::io;
use std::io::{Read, Write};
use std::mem;
use std::time;

use mio::net as mio_net;
use mio::PollOpt;
use mio::Ready;
use mio::Token;

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::env;
use std::net;
use std::net::Shutdown;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::process;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

pub const LOG_DEBUG: u8 = 4;
pub const LOG_INFO: u8 = 3;
pub const LOG_WARN: u8 = 2;
pub const LOG_ERROR: u8 = 1;

#[cfg(test)]
pub const BUF_SIZE: usize = 4;
#[cfg(not(test))]
pub const BUF_SIZE: usize = 65536;

pub const MAX_MSG_BUF_LEN: usize = 128;

// per-thread log level and log format
thread_local!(static LOGLEVEL: RefCell<u8> = RefCell::new(LOG_DEBUG));

pub fn set_loglevel(ll: u8) -> Result<(), String> {
    LOGLEVEL.with(move |level| match ll {
        LOG_ERROR..=LOG_DEBUG => {
            *level.borrow_mut() = ll;
            Ok(())
        }
        _ => Err("Invalid log level".to_string()),
    })
}

pub fn get_loglevel() -> u8 {
    let mut res = 0;
    LOGLEVEL.with(|lvl| {
        res = *lvl.borrow();
    });
    res
}

pub const IDLE_TIMEOUT: u128 = 30 * 1000; // 30 seconds idle without any I/O? kill the socket

macro_rules! debug {
    ($($arg:tt)*) => ({
        if get_loglevel() >= LOG_DEBUG {
            use std::time::SystemTime;
            let (ts_sec, ts_msec) = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                Ok(n) => (n.as_secs(), n.subsec_nanos() / 1_000_000),
                Err(_) => (0, 0)
            };
            eprintln!("DEBUG [{}.{:03}] [{}:{}] {}", ts_sec, ts_msec, file!(), line!(), format!($($arg)*));
        }
    })
}

macro_rules! info {
    ($($arg:tt)*) => ({
        if get_loglevel() >= LOG_INFO {
            use std::time::SystemTime;
            let (ts_sec, ts_msec) = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                Ok(n) => (n.as_secs(), n.subsec_nanos() / 1_000_000),
                Err(_) => (0, 0)
            };
            eprintln!("INFO [{}.{:03}] [{}:{}] {}", ts_sec, ts_msec, file!(), line!(), format!($($arg)*));
        }
    })
}

macro_rules! warn {
    ($($arg:tt)*) => ({
        if get_loglevel() >= LOG_WARN {
            use std::time::SystemTime;
            let (ts_sec, ts_msec) = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                Ok(n) => (n.as_secs(), n.subsec_nanos() / 1_000_000),
                Err(_) => (0, 0)
            };
            eprintln!("WARN [{}.{:03}] [{}:{}] {}", ts_sec, ts_msec, file!(), line!(), format!($($arg)*));
        }
    })
}

macro_rules! error {
    ($($arg:tt)*) => ({
        if get_loglevel() >= LOG_ERROR {
            use std::time::SystemTime;
            let (ts_sec, ts_msec) = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                Ok(n) => (n.as_secs(), n.subsec_nanos() / 1_000_000),
                Err(_) => (0, 0)
            };
            eprintln!("ERROR [{}.{:03}] [{}:{}] {}", ts_sec, ts_msec, file!(), line!(), format!($($arg)*));
        }
    })
}

const SERVER: Token = mio::Token(0);

pub fn get_epoch_time_ms() -> u128 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    return since_the_epoch.as_millis();
}

#[derive(Debug)]
pub enum Error {
    /// Failed to read
    ReadError(io::Error),
    /// Failed to write
    WriteError(io::Error),
    /// Blocked
    Blocked,
    /// Receive timed out
    RecvTimeout,
    /// Not connected to peer
    ConnectionBroken,
    /// Connection could not be (re-)established
    ConnectionError,
    /// Failed to bind
    BindError,
    /// Failed to poll
    PollError,
    /// Failed to accept
    AcceptError,
    /// Failed to register socket with poller
    RegisterError,
    /// Failed to query socket metadata
    SocketError,
    /// server is not bound to a socket
    NotConnected,
    /// Stream is sending too fast
    BurstError,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::ReadError(ref io) => fmt::Display::fmt(io, f),
            Error::WriteError(ref io) => fmt::Display::fmt(io, f),
            Error::RecvTimeout => write!(f, "Packet receive timeout"),
            Error::ConnectionBroken => write!(f, "connection to peer node is broken"),
            Error::ConnectionError => write!(f, "connection to peer could not be (re-)established"),
            Error::BindError => write!(f, "Failed to bind to the given address"),
            Error::PollError => write!(f, "Failed to poll"),
            Error::AcceptError => write!(f, "Failed to accept connection"),
            Error::RegisterError => write!(f, "Failed to register socket with poller"),
            Error::SocketError => write!(f, "Socket error"),
            Error::NotConnected => write!(f, "Not connected to peer network"),
            Error::Blocked => write!(f, "EWOULDBLOCK"),
            Error::BurstError => write!(f, "Stream is sending too much data"),
        }
    }
}

impl error::Error for Error {
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            Error::ReadError(ref io) => Some(io),
            Error::WriteError(ref io) => Some(io),
            Error::RecvTimeout => None,
            Error::ConnectionBroken => None,
            Error::ConnectionError => None,
            Error::BindError => None,
            Error::PollError => None,
            Error::AcceptError => None,
            Error::RegisterError => None,
            Error::SocketError => None,
            Error::NotConnected => None,
            Error::Blocked => None,
            Error::BurstError => None,
        }
    }
}

/// Polling state from the network.
/// Binds event IDs to newly-accepted sockets.
/// Lists event IDs with I/O readiness.
pub struct NetworkPollState {
    pub new: HashMap<usize, mio_net::TcpStream>,
    pub ready: Vec<usize>,
}

impl NetworkPollState {
    pub fn new() -> NetworkPollState {
        NetworkPollState {
            new: HashMap::new(),
            ready: vec![],
        }
    }
}

/// State of the network.
pub struct NetworkState {
    poll: mio::Poll,
    server: mio_net::TcpListener,
    events: mio::Events,
    count: usize,
}

/// The "message type" field in a Bitcoin message header.  Always 12 bytes; contains a
/// null-terminated ASCII string with the request.
#[derive(Debug, Clone, PartialEq)]
pub struct BitcoinCommand(pub [u8; 12]);

impl BitcoinCommand {
    pub fn parse(s: &str) -> Option<BitcoinCommand> {
        if s.len() > 12 {
            None
        } else {
            let mut bytes = [0u8; 12];
            let strbytes = s.as_bytes();
            for i in 0..strbytes.len() {
                bytes[i] = strbytes[i];
            }
            Some(BitcoinCommand(bytes))
        }
    }

    pub fn to_string(&self) -> String {
        let mut printable = vec![];
        for i in 0..12 {
            if self.0[i] != 0 {
                printable.push(self.0[i]);
            } else {
                break;
            }
        }
        String::from_utf8(printable).unwrap_or("#UNKNOWN".to_string())
    }
}

impl fmt::Display for BitcoinCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

/// Firewall forwarding state.
/// Used both for relaying messages from downstream clients to the upstream bitcoind,
/// and from the upstream bitcoind back down to a downstream client.
pub struct ForwardState {
    /// Buffer to store an incoming Bitcoin message header ("preamble")
    preamble_buf: Vec<u8>,
    /// Buffer to store outgoing bytes to be sent (preamble and payload)
    recv_buf: Vec<u8>,
    /// List of messages received that need to be sent upstream
    messages_to_send: VecDeque<Vec<u8>>,
    /// Pointer into the head of the messages_to_send list
    send_ptr: usize,
    /// Parsed Bitcoin message header.  Instantiated only if we're reading the payload.
    preamble: Option<BitcoinPreamble>,
    /// Number of bytes consumed from the message payload.
    num_read: u64,
    /// Whether or not the message will be dropped.
    filtered: bool,
    /// Whether or not the message has been considered for filtration
    filter_considered: bool,
    /// Timestamp, in milliseconds since the Epoch, when we last read or wrote at least 1 byte
    /// using this forwarding state.
    last_io_at: u128,
    /// Forwarding state is associated with *two* sockets -- the downstream client socket, and the
    /// upstream bitcoind socket.  This is the event ID of the _other_ socket this forwarding state
    /// is bound to in the BitcoinProxy struct below.
    pair_event_id: usize,
}

pub struct BitcoinProxy {
    /// What are the expected magic bytes in each Bitcoin message?
    magic: u32,
    /// Network I/O state
    network: NetworkState,
    /// Address of the upstream bitcoind node
    backend_addr: SocketAddr,
    /// List of Bitcoin commands that are prohibited.  All else are allowed.  If this has items,
    /// then allow_list will be empty.
    deny_list: Vec<BitcoinCommand>,
    /// List of Bitcoin commands that are allowed.  All else are pohibited.  If this has items,
    /// then deny_list will be empty.
    allow_list: Vec<BitcoinCommand>,

    /// Map event IDs to their downstream sockets and their forwarding states.
    downstream: HashMap<usize, mio_net::TcpStream>,
    messages_downstream: HashMap<usize, ForwardState>,

    /// Map event IDs to their upstream sockets and their forwarding states.
    upstream: HashMap<usize, mio_net::TcpStream>,
    messages_upstream: HashMap<usize, ForwardState>,
}

/// Bitcoin message header.  Note that it's fixed-sized.
#[derive(Debug, PartialEq)]
pub struct BitcoinPreamble {
    magic: u32,
    command: BitcoinCommand,
    length: u32,
    checksum: u32,
}

impl BitcoinPreamble {
    #[cfg(test)]
    pub fn new(magic: u32, command: &str, length: u32, checksum: u32) -> BitcoinPreamble {
        BitcoinPreamble {
            magic: magic,
            command: BitcoinCommand::parse(command).unwrap(),
            length: length,
            checksum: checksum,
        }
    }
}

impl NetworkState {
    /// Start listening on the given address.
    fn bind_address(addr: &SocketAddr) -> Result<mio_net::TcpListener, Error> {
        mio_net::TcpListener::bind(addr).map_err(|e| {
            error!("Failed to bind to {:?}: {:?}", addr, e);
            Error::BindError
        })
    }

    /// Allocate the next unused network ID.  Note that there is no roll-over logic (but on 64-bit
    /// systems, usize is a 64-bit integer, so this really shouldn't be a problem).
    pub fn next_event_id(&mut self) -> usize {
        let cnt = self.count;
        self.count += 1;
        cnt
    }

    /// Instantiate the network state.  Bind to the given address, and accept at most `capacity`
    /// sockets.
    pub fn bind(addr: &SocketAddr, capacity: usize) -> Result<NetworkState, Error> {
        let server = NetworkState::bind_address(addr)?;
        let poll = mio::Poll::new().map_err(|e| {
            error!("Failed to initialize poller: {:?}", e);
            Error::BindError
        })?;

        let events = mio::Events::with_capacity(capacity);

        poll.register(&server, SERVER, mio::Ready::all(), mio::PollOpt::edge())
            .map_err(|e| {
                error!("Failed to register server socket: {:?}", &e);
                Error::BindError
            })?;

        Ok(NetworkState {
            poll: poll,
            server: server,
            events: events,
            count: 1,
        })
    }

    /// Register a socket for read/write notifications with this poller.  Poll events will be keyed
    /// to the given event ID.  Use edge triggers instead of level triggers, since that's the only
    /// portable way to get events in mio.
    pub fn register(&mut self, event_id: usize, sock: &mio_net::TcpStream) -> Result<(), Error> {
        debug!("Register socket {:?} as event {}", sock, event_id);
        self.poll
            .register(sock, mio::Token(event_id), Ready::all(), PollOpt::edge())
            .map_err(|e| {
                error!("Failed to register socket: {:?}", &e);
                Error::RegisterError
            })
    }

    /// Deregister a socket event.  Future events on the socket will be ignored, and the socket
    /// will be explicitly shutdown.
    pub fn deregister(&mut self, sock: &mio_net::TcpStream) -> Result<(), Error> {
        self.poll.deregister(sock).map_err(|e| {
            error!("Failed to deregister socket: {:?}", &e);
            Error::RegisterError
        })?;

        sock.shutdown(Shutdown::Both)
            .map_err(|_e| Error::SocketError)?;

        debug!("Socket disconnected: {:?}", sock);
        Ok(())
    }

    /// Poll socket states.  Create a new NetworkPollState struct to store newly-accepted sockets,
    /// and list ready sockets.
    pub fn poll(&mut self, timeout: u64) -> Result<NetworkPollState, Error> {
        self.poll
            .poll(&mut self.events, Some(Duration::from_millis(timeout)))
            .map_err(|e| {
                error!("Failed to poll: {:?}", &e);
                Error::PollError
            })?;

        let mut poll_state = NetworkPollState::new();
        for event in &self.events {
            match event.token() {
                SERVER => {
                    // new inbound connection(s)
                    loop {
                        let (client_sock, _client_addr) = match self.server.accept() {
                            Ok((client_sock, client_addr)) => (client_sock, client_addr),
                            Err(e) => match e.kind() {
                                io::ErrorKind::WouldBlock => {
                                    break;
                                }
                                _ => {
                                    return Err(Error::AcceptError);
                                }
                            },
                        };

                        debug!(
                            "New socket accepted from {:?} (event {}): {:?}",
                            &_client_addr, self.count, &client_sock
                        );
                        poll_state.new.insert(self.count, client_sock);
                        self.count += 1;
                    }
                }
                mio::Token(event_id) => {
                    // I/O available
                    poll_state.ready.push(event_id);
                }
            }
        }

        Ok(poll_state)
    }
}

impl ForwardState {
    pub fn new() -> ForwardState {
        ForwardState {
            preamble_buf: vec![],
            recv_buf: vec![],
            send_ptr: 0,
            messages_to_send: VecDeque::new(),
            preamble: None,
            num_read: 0,
            filtered: false,
            filter_considered: false,
            last_io_at: get_epoch_time_ms(),
            pair_event_id: 0,
        }
    }

    /// Reset the forwarding state.  Called each time we process a whole message.
    pub fn reset_write(&mut self) -> () {
        debug!("Reset write!");
        self.messages_to_send.pop_front();
        self.send_ptr = 0;
    }

    pub fn reset_read(&mut self) -> Result<(), Error> {
        if let Some(preamble) = self.preamble.as_ref() {
            debug!(
                "Reset read of message '{}': filtered = {}",
                &preamble.command, &self.filtered
            );
        } else {
            debug!("Reset read! Filtered = {}", self.filtered);
        }
        if !self.filtered {
            if self.messages_to_send.len() > MAX_MSG_BUF_LEN {
                error!("Too many messages from event {}", self.pair_event_id);
                return Err(Error::BurstError);
            }

            let buf = mem::replace(&mut self.recv_buf, vec![]);
            self.messages_to_send.push_back(buf);
        } else {
            self.recv_buf.clear();
        }
        self.preamble_buf.clear();
        self.preamble = None;
        self.num_read = 0;
        self.filtered = false;
        self.filter_considered = false;
        Ok(())
    }

    /// Given the expected magic and byte buffer encoding a Bitcoin message header, parse it out.
    /// Returns None if the bytes do not encode a well-formed header.
    /// The checksum field will be ignored.
    fn parse_preamble(magic: u32, buf: &[u8]) -> Option<BitcoinPreamble> {
        if buf.len() < mem::size_of::<BitcoinPreamble>() {
            debug!(
                "Invalid preamble: buf is {}, expected {}",
                buf.len(),
                mem::size_of::<BitcoinPreamble>()
            );
            return None;
        }

        let mut msg_magic_bytes = [0u8; 4];
        msg_magic_bytes.copy_from_slice(&buf[0..4]);

        let msg_magic = u32::from_le_bytes(msg_magic_bytes);
        if msg_magic != magic {
            debug!("Invalid magic: expected {:x}, got {:x}", magic, msg_magic);
            return None;
        }

        let mut command_bytes = [0u8; 12];
        command_bytes.copy_from_slice(&buf[4..16]);
        let command = BitcoinCommand(command_bytes);

        let mut msg_len_bytes = [0u8; 4];
        let mut msg_checksum_bytes = [0u8; 4];

        msg_len_bytes.copy_from_slice(&buf[16..20]);
        msg_checksum_bytes.copy_from_slice(&buf[20..24]);

        let msglen = u32::from_le_bytes(msg_len_bytes);
        let checksum = u32::from_le_bytes(msg_checksum_bytes);

        info!("Received '{}' of {} bytes", &command, msglen);

        Some(BitcoinPreamble {
            magic: magic,
            command: command,
            length: msglen,
            checksum: checksum,
        })
    }

    /// Attempt to drain the write buffer to the given writeable upstream.  Returns Ok(true) if we
    /// drain the buffer, Ok(false) if there are bytes remaining, or Err(...) on network error.
    pub fn try_flush<W: Write>(&mut self, upstream: &mut W) -> Result<bool, Error> {
        if self.messages_to_send.len() == 0 {
            return Ok(true);
        }

        let write_buf = self.messages_to_send.front().unwrap();
        if self.send_ptr >= write_buf.len() {
            debug!(
                "Write buffer is empty: {} >= {}",
                self.send_ptr,
                write_buf.len()
            );
            self.reset_write();
            return Ok(self.messages_to_send.len() == 0);
        }

        let max_write = if self.send_ptr + BUF_SIZE < write_buf.len() {
            BUF_SIZE
        } else {
            write_buf.len() - self.send_ptr
        };

        debug!("Try to write up to {} bytes", &max_write);
        let to_flush = &write_buf[self.send_ptr..(self.send_ptr + max_write)];
        let nw = upstream.write(to_flush).map_err(|e| match e.kind() {
            io::ErrorKind::WouldBlock => Error::Blocked,
            _ => Error::WriteError(e),
        })?;

        if nw > 0 {
            self.last_io_at = get_epoch_time_ms();
        }

        debug!(
            "Forwarded {} bytes upstream, ptr={}, len={}",
            nw,
            self.send_ptr,
            write_buf.len()
        );
        self.send_ptr += nw;

        if self.send_ptr >= write_buf.len() {
            debug!("Clear flushed write buffer ({} bytes)", write_buf.len());
            self.reset_write();
            return Ok(self.messages_to_send.len() == 0);
        } else {
            return Ok(false);
        }
    }

    /// Read a Bitcoin message header from the readable fd, with the given magic bytes.
    /// Reads up to, but not over, the number of bytes in a Bitcoin message header.
    /// If we get enough bytes to form a Bitcoin message header, and if we successfully parse it
    /// into a BitcoinPreamble, then return Ok(true) so we can proceed to read the payload.
    /// Return Ok(false) if we don't have enough bytes to form a BitcoinPreamble.
    /// Return Err(...) on network error, or if we can't parse a BitcoinPreamble (indicates to the
    /// network poll loop to disconnect this socket).
    pub fn read_preamble<R: Read>(&mut self, magic: u32, fd: &mut R) -> Result<bool, Error> {
        assert!(self.preamble.is_none());
        if self.preamble_buf.len() >= mem::size_of::<BitcoinPreamble>() {
            // should be unreachable
            return Err(Error::ConnectionBroken);
        }

        let to_read = mem::size_of::<BitcoinPreamble>() - self.preamble_buf.len();
        let mut buf = vec![0u8; to_read];

        debug!("Read up to {} preamble bytes", to_read);
        let nr = match fd.read(&mut buf) {
            Ok(0) => {
                return Err(Error::ConnectionBroken);
            }
            Ok(nr) => {
                debug!("Read {} preamble bytes out of {}", nr, to_read);
                self.last_io_at = get_epoch_time_ms();
                nr
            }
            Err(e) => match e.kind() {
                io::ErrorKind::WouldBlock => {
                    debug!("Reader is blocked");
                    return Err(Error::Blocked);
                }
                _ => {
                    return Err(Error::ReadError(e));
                }
            },
        };

        self.preamble_buf.extend_from_slice(&buf[0..nr]);
        if self.preamble_buf.len() == mem::size_of::<BitcoinPreamble>() {
            let preamble = ForwardState::parse_preamble(magic, &self.preamble_buf);
            if preamble.is_none() {
                return Err(Error::ConnectionBroken);
            }

            // forward premable upstream
            self.recv_buf.extend_from_slice(&self.preamble_buf);

            self.preamble = preamble;
            self.preamble_buf.clear();
            Ok(true)
        } else {
            debug!(
                "Reader has read {} preamble bytes ({} total)",
                nr,
                self.preamble_buf.len()
            );
            Ok(false)
        }
    }

    /// Consume a Bitcoin message payload from the given readable input.  
    /// Buffers up the bytes into the write buffer, so a subsequent call to .try_flush() can be
    /// used to send them to the paired socket.
    /// Returns the number of bytes read on success.  Returns Err(...) on network error, in which case, the underlying
    /// socket will be closed.
    pub fn read_payload<R: Read>(&mut self, input: &mut R) -> Result<usize, Error> {
        assert!(self.preamble.is_some());

        let preamble = self.preamble.as_ref().unwrap();
        assert!(self.num_read <= (preamble.length as u64));

        let to_read = (preamble.length as u64) - self.num_read;
        let max_read = if to_read < BUF_SIZE as u64 {
            to_read as usize
        } else {
            BUF_SIZE as usize
        };

        debug!("Read up to {} bytes", &max_read);

        let mut buf = [0u8; BUF_SIZE];
        let nr = match input.read(&mut buf[0..max_read]) {
            Ok(0) => {
                if max_read > 0 {
                    debug!("EOF encountered");
                    return Err(Error::ConnectionBroken);
                } else {
                    // zero-length message was expected.
                    // Can happen with `verack`, for example.
                    debug!(
                        "Read zero-length payload (so-far: {}, total: {}, max: {})",
                        self.num_read, preamble.length, max_read
                    );
                    self.last_io_at = get_epoch_time_ms();
                    0
                }
            }
            Ok(nr) => {
                debug!(
                    "Read {} payload bytes out of {} (so-far: {}, total: {})",
                    nr, max_read, self.num_read, preamble.length
                );
                self.last_io_at = get_epoch_time_ms();
                nr
            }
            Err(e) => match e.kind() {
                io::ErrorKind::WouldBlock => {
                    return Err(Error::Blocked);
                }
                _ => {
                    return Err(Error::ReadError(e));
                }
            },
        };

        self.recv_buf.extend_from_slice(&buf[0..nr]);
        self.num_read += nr as u64;

        if self.num_read >= (preamble.length as u64) {
            self.reset_read()?;
        }

        Ok(nr)
    }

    /// Read preambles and messages until blocked.
    /// Only log filtering for downstream messages (i.e. messages sent to us from another peer)
    pub fn read_until_blocked<R: Read>(
        &mut self,
        magic: u32,
        input: &mut R,
        allow_list: &[BitcoinCommand],
        deny_list: &[BitcoinCommand],
        is_downstream_message: bool,
    ) -> bool {
        let mut blocked = false;
        while !blocked {
            if self.preamble.is_none() {
                match self.read_preamble(magic, input) {
                    Ok(_) => {}
                    Err(Error::Blocked) => {
                        debug!("read_preamble blocked");
                        blocked = true;
                    }
                    Err(_e) => {
                        return false;
                    }
                }
            }

            if let Some(preamble) = self.preamble.as_ref() {
                if !self.filtered
                    && BitcoinProxy::filter(
                        &self.preamble.as_ref().unwrap(),
                        allow_list,
                        deny_list,
                        is_downstream_message,
                    )
                {
                    self.filtered = true;
                }
                if !self.filtered && !self.filter_considered {
                    if is_downstream_message {
                        info!("Allow message '{}'", &preamble.command);
                    }
                    self.filter_considered = true;
                }

                match self.read_payload(input) {
                    Ok(_) => {}
                    Err(Error::Blocked) => {
                        debug!("read_payload blocked");
                        blocked = true;
                    }
                    Err(_e) => {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    /// Send out messages until blocked
    pub fn write_until_blocked<W: Write>(&mut self, output: &mut W) -> bool {
        let mut blocked = false;
        while !blocked {
            match self.try_flush(output) {
                Ok(drained) => {
                    if drained {
                        // nothing more to send
                        blocked = true;
                    }
                }
                Err(Error::Blocked) => {
                    debug!("try_flush blocked");
                    blocked = true;
                }
                Err(_e) => {
                    return false;
                }
            }
        }
        return true;
    }

    /// Has the socket gone more than IDLE_TIMEOUT milliseconds without reading or writing
    /// anything?  Used to disconnect idle sockets.
    pub fn is_idle_timeout(&self) -> bool {
        if self.last_io_at + IDLE_TIMEOUT < get_epoch_time_ms() {
            return true;
        } else {
            return false;
        }
    }
}

impl BitcoinProxy {
    /// Create a new BitcoinProxy, bound to the given port, and forwarding packets to the given
    /// backend socket address.  Binds a server socket on success.
    pub fn new(magic: u32, port: u16, backend_addr: SocketAddr) -> Result<BitcoinProxy, Error> {
        let net = NetworkState::bind(
            &format!("0.0.0.0:{}", port).parse::<SocketAddr>().unwrap(),
            1000,
        )?;
        Ok(BitcoinProxy {
            network: net,
            magic: magic,
            backend_addr: backend_addr,
            deny_list: vec![],
            allow_list: vec![],
            messages_downstream: HashMap::new(),
            downstream: HashMap::new(),
            messages_upstream: HashMap::new(),
            upstream: HashMap::new(),
        })
    }

    /// Add a command to the firewall allow_list.
    /// Duplicates are not checked.
    pub fn add_allow_list(&mut self, cmd: BitcoinCommand) -> () {
        self.allow_list.push(cmd);
    }

    /// Adds a command to the firewall deny list.
    /// Duplicates are not checked.
    pub fn add_deny_list(&mut self, cmd: BitcoinCommand) -> () {
        self.deny_list.push(cmd);
    }

    /// Given a preamble, a allow list, and a deny list, determine whether or not to filter the
    /// Bitcoin message (returns true to filter; false not to).
    /// While this is not checked, either allow_list, deny_list, or both must have zero entries for
    /// this to work as expected.  If both lists are non-empty, the allow_list is preferred.
    ///
    /// Only log drops if is_downstream_message is true.
    ///
    /// Returns true if the message is to be filtered.
    /// Returns false if not.
    pub fn filter(
        preamble: &BitcoinPreamble,
        allow_list: &[BitcoinCommand],
        deny_list: &[BitcoinCommand],
        is_downstream_message: bool,
    ) -> bool {
        if allow_list.len() > 0 {
            for cmd in allow_list.iter() {
                if *cmd == preamble.command {
                    return false;
                }
            }
            if is_downstream_message {
                info!("Drop non-allow-listed command '{:?}'", &preamble.command,);
            }
            return true;
        }

        if deny_list.len() > 0 {
            for cmd in deny_list.iter() {
                if *cmd == preamble.command {
                    if is_downstream_message {
                        info!("Drop deny-listed command '{:?}'", &preamble.command);
                    }
                    return true;
                }
            }

            return false;
        }

        return false;
    }

    /// Register a downstream client connection.
    /// This is called to handle a recently-accepted socket.
    /// Opens an associated socket to the upstream bitcoind, so that messages from this socket are
    /// sent out to bitcoind if they are not filtered.
    /// Registers both sockets with the network poller, and creates forwarding state for both.
    /// Each socket's forwarding state contains is the other's paired event ID, which will be
    /// necessary to find the right socket to ferry bytes to when processing socket I/O.
    fn register_socket(
        &mut self,
        event_id: usize,
        socket: mio_net::TcpStream,
    ) -> Result<(), Error> {
        socket
            .set_nodelay(true)
            .map_err(|_e| Error::ConnectionError)?;
        socket
            .set_send_buffer_size(BUF_SIZE)
            .map_err(|_e| Error::ConnectionError)?;
        socket
            .set_recv_buffer_size(BUF_SIZE)
            .map_err(|_e| Error::ConnectionError)?;
        socket
            .set_linger(Some(time::Duration::from_millis(5000)))
            .map_err(|_e| Error::ConnectionError)?;

        let upstream_sync =
            net::TcpStream::connect_timeout(&self.backend_addr, Duration::new(1, 0))
                .map_err(|_e| Error::ConnectionError)?;
        let upstream =
            mio_net::TcpStream::from_stream(upstream_sync).map_err(|_e| Error::ConnectionError)?;

        upstream
            .set_nodelay(true)
            .map_err(|_e| Error::ConnectionError)?;
        upstream
            .set_send_buffer_size(BUF_SIZE)
            .map_err(|_e| Error::ConnectionError)?;
        upstream
            .set_recv_buffer_size(BUF_SIZE)
            .map_err(|_e| Error::ConnectionError)?;
        upstream
            .set_linger(Some(time::Duration::from_millis(5000)))
            .map_err(|_e| Error::ConnectionError)?;

        let upstream_event_id = self.network.next_event_id();

        self.network.register(event_id, &socket)?;
        self.network
            .register(upstream_event_id, &upstream)
            .map_err(|e| {
                let _ = self.network.deregister(&socket);
                e
            })?;

        let mut forward = ForwardState::new();
        let mut backward = ForwardState::new();

        forward.pair_event_id = upstream_event_id;
        backward.pair_event_id = event_id;

        self.messages_downstream.insert(event_id, forward);
        self.downstream.insert(event_id, socket);

        self.messages_upstream.insert(upstream_event_id, backward);
        self.upstream.insert(upstream_event_id, upstream);

        debug!("Registered event pair {},{}", event_id, upstream_event_id);
        Ok(())
    }

    /// Disconnect an event's socket from the poller, and free up its forwarding state.
    /// Also closes the paired socket.
    fn deregister_socket(&mut self, event_id: usize) -> () {
        if let Some(fwd) = self.messages_downstream.remove(&event_id) {
            if let Some(sock) = self.downstream.get(&event_id) {
                let _ = self.network.deregister(sock);
            }

            let mut remove_upstream = false;
            if let Some(sock) = self.upstream.get(&fwd.pair_event_id) {
                let _ = self.network.deregister(sock);
                remove_upstream = true;
            }

            self.downstream.remove(&event_id);
            if remove_upstream {
                self.upstream.remove(&fwd.pair_event_id);
            }
        }

        if let Some(fwd) = self.messages_upstream.remove(&event_id) {
            if let Some(sock) = self.upstream.get(&event_id) {
                let _ = self.network.deregister(sock);
            }

            let mut remove_downstream = false;
            if let Some(sock) = self.downstream.get(&fwd.pair_event_id) {
                let _ = self.network.deregister(sock);
                remove_downstream = true;
            }

            self.upstream.remove(&event_id);
            if remove_downstream {
                self.downstream.remove(&fwd.pair_event_id);
            }
        }

        debug!("De-egistered event {}", event_id);
    }

    /// Register all new downstream clients, and open an upstream socket to bitcoind for each one.
    pub fn process_new_sockets(&mut self, pollstate: &mut NetworkPollState) -> () {
        for (event_id, sock) in pollstate.new.drain() {
            let _ = self.register_socket(event_id, sock);
        }
    }

    /// Handle socket I/O from one socket (inbound) to another (outbound), filtering messages as
    /// needed.  Handles as many bytes as possible, bufferring them up into the inbound socket's
    /// forwarding state as needed.  Runs until encountering EWOULDBLOCK on both inbound and
    /// outbound.
    ///
    /// Logs message filtering if is_downstream_message is true
    pub fn process_socket_io<R: Read, W: Write>(
        magic: u32,
        allow_list: &[BitcoinCommand],
        deny_list: &[BitcoinCommand],
        inbound: &mut R,
        forward: &mut ForwardState,
        outbound: &mut W,
        is_downstream_message: bool,
    ) -> bool {
        if !forward.read_until_blocked(magic, inbound, allow_list, deny_list, is_downstream_message)
        {
            return false;
        }
        if !forward.write_until_blocked(outbound) {
            return false;
        }
        return true;
    }

    /// Handle I/O on all ready sockets.  Read bytes from downstream clients and write them to
    /// their upstream sockets, and read bytes from upstream sockets and write them to downstream
    /// clients.  The filter will only be applied to messages coming from downstream clients.
    pub fn process_ready_sockets(&mut self, pollstate: &mut NetworkPollState) -> () {
        let mut broken = vec![];
        for event_id in pollstate.ready.iter() {
            if self.downstream.get(event_id).is_some() {
                debug!("Event {} (a downstream socket) is ready for I/O", event_id);
                let pair_event_id = match self.messages_downstream.get(event_id) {
                    Some(ref fwd) => fwd.pair_event_id,
                    None => {
                        continue;
                    }
                };

                match (
                    self.messages_downstream.get_mut(event_id),
                    self.downstream.get_mut(event_id),
                    self.upstream.get_mut(&pair_event_id),
                ) {
                    (Some(ref mut forward), Some(ref mut downstream), Some(ref mut upstream)) => {
                        debug!(
                            "Process I/O from downstream event {} ({:?})",
                            *event_id, downstream
                        );
                        let alive = BitcoinProxy::process_socket_io(
                            self.magic,
                            &self.allow_list,
                            &self.deny_list,
                            downstream,
                            forward,
                            upstream,
                            true,
                        );
                        if !alive {
                            broken.push(*event_id);
                        }
                    }
                    (_, _, _) => {
                        continue;
                    }
                }
            } else if self.upstream.get(event_id).is_some() {
                debug!("Event {} (an upstream socket) is ready for I/O", event_id);
                let pair_event_id = match self.messages_upstream.get(event_id) {
                    Some(ref fwd) => fwd.pair_event_id,
                    None => {
                        continue;
                    }
                };

                match (
                    self.messages_upstream.get_mut(event_id),
                    self.downstream.get_mut(&pair_event_id),
                    self.upstream.get_mut(event_id),
                ) {
                    (Some(ref mut forward), Some(ref mut downstream), Some(ref mut upstream)) => {
                        debug!(
                            "Process I/O from upstream event {} ({:?})",
                            *event_id, downstream
                        );
                        let alive = BitcoinProxy::process_socket_io(
                            self.magic,
                            &vec![],
                            &vec![],
                            upstream,
                            forward,
                            downstream,
                            false,
                        );
                        if !alive {
                            broken.push(*event_id);
                        }
                    }
                    (_, _, _) => {
                        continue;
                    }
                }
            }
        }

        for event_id in broken {
            self.deregister_socket(event_id);
        }
    }

    /// Find idle sockets and disconnect them.
    pub fn process_dead_sockets(&mut self) -> () {
        let mut timedout: Vec<usize> = vec![];
        for event_id in self.messages_downstream.keys() {
            match self.messages_downstream.get(event_id) {
                Some(ref fwd) => {
                    if fwd.is_idle_timeout() {
                        debug!("Event {} (downstream) has idled too long", event_id);
                        timedout.push(*event_id);
                    }
                }
                None => {}
            }
        }
        for event_id in self.messages_upstream.keys() {
            match self.messages_upstream.get(event_id) {
                Some(ref fwd) => {
                    if fwd.is_idle_timeout() {
                        debug!("Event {} (upstream) has idled too long", event_id);
                        timedout.push(*event_id);
                    }
                }
                None => {}
            }
        }
        for event_id in timedout {
            self.deregister_socket(event_id);
        }
    }

    /// Do one poll pass.
    pub fn poll(&mut self, timeout: u64) -> Result<(), Error> {
        debug!(
            "<<<<<<<<<<<<<<<<< Begin poll ({}) <<<<<<<<<<<<<<<<<<<<<<<",
            timeout
        );
        let mut poll_state = self.network.poll(timeout)?;
        debug!("<<<<<<<<<<<<<<<<<<< End poll <<<<<<<<<<<<<<<<<<<<<<<");

        self.process_new_sockets(&mut poll_state);
        self.process_ready_sockets(&mut poll_state);
        self.process_dead_sockets();
        Ok(())
    }
}

/// Given a mutable list of arguments, and a switch, find the first instance of the switch and its
/// associated argument, remove them from argv, and return the argument itself.  Returns None if
/// not found.
fn find_arg_value(arg: &str, argv: &mut Vec<String>) -> Option<String> {
    debug!("Find '{}'. Argv is {:?}", arg, argv);
    let mut idx: i64 = -1;
    for i in 0..argv.len() - 1 {
        if argv[i] == arg {
            idx = (i + 1) as i64;
            break;
        }
    }
    if idx > 0 {
        let value = argv[idx as usize].to_string();
        debug!("Argument {} is '{}'", idx, &value);

        argv.remove((idx - 1) as usize);
        argv.remove((idx - 1) as usize);
        Some(value)
    } else {
        None
    }
}

/// Print usage and exit
fn usage(prog_name: String) -> () {
    eprintln!(
        "Usage: {} [-w|-b MSG1,MSG2,MSG3,...] [-v VERBOSITY] [-n NETWORK-NAME] port upstream_addr",
        prog_name
    );
    process::exit(1);
}

// Entry point
fn main() {
    set_loglevel(LOG_DEBUG).unwrap();

    let mut argv: Vec<String> = env::args().map(|s| s.to_string()).collect();
    let prog_name = argv[0].clone();

    if argv.len() < 2 {
        usage(prog_name.clone());
    }

    // find -v
    let verbosity_arg = find_arg_value("-v", &mut argv).unwrap_or(format!("{}", LOG_ERROR));
    let verbosity = verbosity_arg
        .parse::<u8>()
        .map_err(|_e| usage(prog_name.clone()))
        .unwrap();

    let _ = set_loglevel(verbosity);

    // find -a
    let allow_list_filter_csv = find_arg_value("-a", &mut argv).unwrap_or("".to_string());
    let mut allow_list_filter: Vec<String> = allow_list_filter_csv
        .split(",")
        .filter(|s| s.len() > 0)
        .map(|s| s.to_string())
        .collect();

    // find -d
    let deny_list_filter_csv = find_arg_value("-d", &mut argv).unwrap_or("".to_string());
    let mut deny_list_filter: Vec<String> = deny_list_filter_csv
        .split(",")
        .filter(|s| s.len() > 0)
        .map(|s| s.to_string())
        .collect();

    if allow_list_filter.len() != 0 && deny_list_filter.len() != 0 {
        debug!("allow_list filter: {:?}", &allow_list_filter);
        debug!("deny_list filter: {:?}", &deny_list_filter);
        usage(prog_name.clone());
    }

    // find -n
    let network_name = find_arg_value("-n", &mut argv).unwrap_or("mainnet".to_string());
    let magic = match network_name.as_str() {
        "mainnet" => 0xD9B4BEF9,
        "testnet" => 0x0709110B,
        "regtest" => 0xDAB5BFFA,
        _ => {
            usage(prog_name.clone());
            unreachable!();
        }
    };

    debug!("Final Argv is {:?}", &argv);

    // find port
    if argv.len() != 3 {
        usage(prog_name.clone());
    }
    let port = argv[1]
        .parse::<u16>()
        .map_err(|_e| usage(prog_name.clone()))
        .unwrap();
    let backend_str = &argv[2];
    let backend = match backend_str.parse::<SocketAddr>() {
        Ok(backend) => backend,
        Err(_e) => match backend_str.as_str().to_socket_addrs() {
            Ok(mut addrs) => match addrs.next() {
                Some(addr) => addr,
                None => {
                    usage(prog_name.clone());
                    unreachable!();
                }
            },
            Err(_e) => {
                usage(prog_name.clone());
                unreachable!();
            }
        },
    };

    debug!("Bind on port {}", port);
    let mut proxy = BitcoinProxy::new(magic, port, backend).unwrap();
    debug!("Bound to 0.0.0.0:{}", port);
    debug!("Backend is {}", &backend);

    for message_name in allow_list_filter.drain(..) {
        if message_name.len() > 12 {
            eprintln!("Invalid Bitcoin message type: '{}'", message_name);
            usage(prog_name.clone());
        }
        info!("allow_list '{}'", message_name);
        let bitcoin_cmd = BitcoinCommand::parse(&message_name).unwrap();
        proxy.add_allow_list(bitcoin_cmd);
    }

    for message_name in deny_list_filter.drain(..) {
        if message_name.len() > 12 {
            eprintln!("Invalid Bitcoin message type: '{}'", message_name);
            usage(prog_name.clone());
        }
        info!("deny_list '{}'", message_name);
        let bitcoin_cmd = BitcoinCommand::parse(&message_name).unwrap();
        proxy.add_deny_list(bitcoin_cmd);
    }

    loop {
        match proxy.poll(IDLE_TIMEOUT as u64) {
            Ok(_) => {}
            Err(_e) => {
                break;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Cursor;
    use std::io::{Read, Write};

    /// A Cursor container that will return EWOULDBLOCK after reading a given number of bytes.
    /// Used to test EWOULDBLOCK handling without actually setting up sockets.
    pub struct BlockingCursor<T> {
        inner: Cursor<T>,
        block_window: usize,
        block: bool,
    }

    impl<T: AsRef<[u8]>> BlockingCursor<T> {
        pub fn new(cursor: Cursor<T>, block_window: usize) -> BlockingCursor<T> {
            BlockingCursor {
                inner: cursor,
                block_window: block_window,
                block: false,
            }
        }

        pub fn destruct(self) -> Cursor<T> {
            self.inner
        }
    }

    impl<T: AsRef<[u8]>> Read for BlockingCursor<T> {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            if self.block {
                self.block = false;
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
            self.block = true;
            if bytes.len() > self.block_window {
                self.inner.read(&mut bytes[0..self.block_window])
            } else {
                self.inner.read(bytes)
            }
        }
    }

    impl Write for BlockingCursor<&mut Vec<u8>> {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.block {
                self.block = false;
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
            self.block = true;
            if bytes.len() > self.block_window {
                self.inner.write(&bytes[0..self.block_window])
            } else {
                self.inner.write(bytes)
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            self.inner.flush()
        }
    }

    #[test]
    fn test_parse_preamble() {
        let message_bytes = vec![
            // magic
            0xd9, 0xb4, 0xbe, 0xf9, // command (verack)
            0x76, 0x65, 0x72, 0x61, 0x63, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // length (1234)
            0x0d2, 0x04, 0x00, 0x00, // checksum (0x12345678)
            0x78, 0x56, 0x34, 0x12,
        ];
        let preamble = ForwardState::parse_preamble(0xf9beb4d9, &message_bytes).unwrap();
        assert_eq!(
            preamble,
            BitcoinPreamble::new(0xf9beb4d9, "verack", 1234, 0x12345678)
        );

        // bad magic
        assert!(ForwardState::parse_preamble(0xD9B4BEF8, &message_bytes).is_none());
    }

    #[test]
    fn test_read_message() {
        let message_bytes = vec![
            // magic
            0xd9, 0xb4, 0xbe, 0xf9, // command (verack)
            0x76, 0x65, 0x72, 0x61, 0x63, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // length (16)
            0x10, 0x00, 0x00, 0x00, // checksum (0x78563412)
            0x12, 0x34, 0x56, 0x78, // payload
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ];

        let mut f = ForwardState::new();
        let mut fd = io::Cursor::new(&message_bytes);
        let mut output = vec![];

        let res = f.read_preamble(0xf9beb4d9, &mut fd).unwrap();
        assert!(res, "Failed to read preamble");

        assert!(f.preamble.is_some());
        assert_eq!(
            f.preamble,
            Some(BitcoinPreamble::new(0xf9beb4d9, "verack", 16, 0x78563412))
        );
        assert_eq!(f.num_read, 0);

        let res = f.try_flush(&mut output).unwrap();
        assert!(res);

        let mut total = 0;
        loop {
            match f.read_payload(&mut fd) {
                Ok(sz) => {
                    total += sz;
                    if total >= 16 {
                        break;
                    }
                }
                Err(Error::Blocked) => {
                    break;
                }
                Err(e) => {
                    panic!("{:?}", &e);
                }
            }
        }

        loop {
            let res = f.try_flush(&mut output).unwrap();
            if res {
                break;
            }
        }

        assert_eq!(output, message_bytes);
        assert!(f.preamble.is_none());
        assert_eq!(f.preamble_buf.len(), 0);
        assert_eq!(f.num_read, 0);
    }

    #[test]
    fn test_socket_io() {
        let stream_bytes = vec![
            // magic
            0xd9, 0xb4, 0xbe, 0xf9, // command (version)
            0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00, 0x00, 0x00,
            // length (8)
            0x08, 0x00, 0x00, 0x00, // checksum (0x12345678)
            0x78, 0x56, 0x34, 0x12, // payload
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, // magic
            0xd9, 0xb4, 0xbe, 0xf9, // command (verack)
            0x76, 0x65, 0x72, 0x61, 0x63, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // length (16)
            0x10, 0x00, 0x00, 0x00, // checksum (0x22334455)
            0x55, 0x44, 0x33, 0x22, // payload
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
            0x1e, 0x1f, // magic
            0xd9, 0xb4, 0xbe, 0xf9, // command (ping)
            0x70, 0x69, 0x6e, 0x67, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // length (4)
            0x04, 0x00, 0x00, 0x00, // checksum (0x33445566)
            0x33, 0x44, 0x55, 0x66, // payload
            0x20, 0x21, 0x22, 0x23,
        ];

        let mut f = ForwardState::new();
        let inner_fd = io::Cursor::new(&stream_bytes);
        let mut inner_output = vec![];

        let mut fd = BlockingCursor::new(inner_fd, 10);
        let mut output = BlockingCursor::new(io::Cursor::new(&mut inner_output), 10);

        for _ in 0..stream_bytes.len() {
            BitcoinProxy::process_socket_io(
                0xf9beb4d9,
                &vec![],
                &vec![],
                &mut fd,
                &mut f,
                &mut output,
                true,
            );
        }

        let inner_fd = fd.destruct();
        let inner_output = output.destruct();

        assert_eq!(inner_fd.position(), stream_bytes.len() as u64);
        assert_eq!(**inner_output.get_ref(), stream_bytes);
    }

    #[test]
    fn test_socket_io_allow_list() {
        let stream_bytes = vec![
            // magic
            0xd9, 0xb4, 0xbe, 0xf9, // command (version)
            0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00, 0x00, 0x00,
            // length (8)
            0x08, 0x00, 0x00, 0x00, // checksum (0x12345678)
            0x78, 0x56, 0x34, 0x12, // payload
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, // magic
            0xd9, 0xb4, 0xbe, 0xf9, // command (verack)
            0x76, 0x65, 0x72, 0x61, 0x63, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // length (16)
            0x10, 0x00, 0x00, 0x00, // checksum (0x22334455)
            0x55, 0x44, 0x33, 0x22, // payload
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
            0x1e, 0x1f, // magic
            0xd9, 0xb4, 0xbe, 0xf9, // command (ping)
            0x70, 0x69, 0x6e, 0x67, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // length (4)
            0x04, 0x00, 0x00, 0x00, // checksum (0x33445566)
            0x66, 0x55, 0x44, 0x33, // payload
            0x20, 0x21, 0x22, 0x23,
        ];

        let filtered_stream_bytes = vec![
            // magic
            0xd9, 0xb4, 0xbe, 0xf9, // command (version)
            0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00, 0x00, 0x00,
            // length (8)
            0x08, 0x00, 0x00, 0x00, // checksum (0x12345678)
            0x78, 0x56, 0x34, 0x12, // payload
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, // magic
            0xd9, 0xb4, 0xbe, 0xf9, // command (ping)
            0x70, 0x69, 0x6e, 0x67, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // length (4)
            0x04, 0x00, 0x00, 0x00, // checksum (0x33445566)
            0x66, 0x55, 0x44, 0x33, // payload
            0x20, 0x21, 0x22, 0x23,
        ];

        let mut f = ForwardState::new();
        let inner_fd = io::Cursor::new(&stream_bytes);
        let mut inner_output = vec![];

        let mut fd = BlockingCursor::new(inner_fd, 10);
        let mut output = BlockingCursor::new(io::Cursor::new(&mut inner_output), 10);

        for _ in 0..stream_bytes.len() {
            BitcoinProxy::process_socket_io(
                0xf9beb4d9,
                &vec![
                    BitcoinCommand::parse("version").unwrap(),
                    BitcoinCommand::parse("ping").unwrap(),
                ],
                &vec![],
                &mut fd,
                &mut f,
                &mut output,
                true,
            );
        }

        let inner_fd = fd.destruct();
        let inner_output = output.destruct();

        assert_eq!(inner_fd.position(), stream_bytes.len() as u64);
        assert_eq!(**inner_output.get_ref(), filtered_stream_bytes);
    }

    #[test]
    fn test_socket_io_deny_list() {
        let stream_bytes = vec![
            // magic
            0xd9, 0xb4, 0xbe, 0xf9, // command (version)
            0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00, 0x00, 0x00,
            // length (8)
            0x08, 0x00, 0x00, 0x00, // checksum (0x12345678)
            0x78, 0x56, 0x34, 0x12, // payload
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, // magic
            0xd9, 0xb4, 0xbe, 0xf9, // command (verack)
            0x76, 0x65, 0x72, 0x61, 0x63, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // length (16)
            0x10, 0x00, 0x00, 0x00, // checksum (0x22334455)
            0x55, 0x44, 0x33, 0x22, // payload
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
            0x1e, 0x1f, // magic
            0xd9, 0xb4, 0xbe, 0xf9, // command (ping)
            0x70, 0x69, 0x6e, 0x67, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // length (4)
            0x04, 0x00, 0x00, 0x00, // checksum (0x33445566)
            0x66, 0x55, 0x44, 0x33, // payload
            0x20, 0x21, 0x22, 0x23,
        ];

        let filtered_stream_bytes = vec![
            // magic
            0xd9, 0xb4, 0xbe, 0xf9, // command (version)
            0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x00, 0x00, 0x00, 0x00, 0x00,
            // length (8)
            0x08, 0x00, 0x00, 0x00, // checksum (0x12345678)
            0x78, 0x56, 0x34, 0x12, // payload
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, // magic
            0xd9, 0xb4, 0xbe, 0xf9, // command (ping)
            0x70, 0x69, 0x6e, 0x67, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // length (4)
            0x04, 0x00, 0x00, 0x00, // checksum (0x33445566)
            0x66, 0x55, 0x44, 0x33, // payload
            0x20, 0x21, 0x22, 0x23,
        ];

        let mut f = ForwardState::new();
        let inner_fd = io::Cursor::new(&stream_bytes);
        let mut inner_output = vec![];

        let mut fd = BlockingCursor::new(inner_fd, 10);
        let mut output = BlockingCursor::new(io::Cursor::new(&mut inner_output), 10);

        for _ in 0..stream_bytes.len() {
            BitcoinProxy::process_socket_io(
                0xf9beb4d9,
                &vec![],
                &vec![BitcoinCommand::parse("verack").unwrap()],
                &mut fd,
                &mut f,
                &mut output,
                true,
            );
        }

        let inner_fd = fd.destruct();
        let inner_output = output.destruct();

        assert_eq!(inner_fd.position(), stream_bytes.len() as u64);
        assert_eq!(**inner_output.get_ref(), filtered_stream_bytes);
    }
}
