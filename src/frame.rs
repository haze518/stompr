use std::{borrow::Cow, io::Read};
use std::collections::HashMap;
use std::io::Cursor;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{Error, ErrorKind};

#[derive(Clone, Debug)]
pub struct Frame {
    pub command: CommandKind,
    // todo replace hashmap
    pub headers: HashMap<String, String>,
    pub payload: Bytes,
}

#[derive(Debug, PartialEq, Clone)]
pub enum CommandKind {
    Connect,
    Stop,
    Connected,
    Send,
    Subscribe,
    Unsubscribe,
    Ack,
    Nack,
    Begin,
    Commit,
    Abort,
    Disconnect,
    Message,
    Receipt,
    Error,
}

impl CommandKind {
    fn as_bytes(&self) -> &'static [u8] {
        match self {
            CommandKind::Connect => b"CONNECT",
            CommandKind::Stop => b"STOP",
            CommandKind::Connected => b"CONNECTED",
            CommandKind::Send => b"SEND",
            CommandKind::Subscribe => b"SUBSCRIBE",
            CommandKind::Unsubscribe => b"UNSUBSCRIBE",
            CommandKind::Ack => b"ACK",
            CommandKind::Nack => b"NACK",
            CommandKind::Begin => b"BEGIN",
            CommandKind::Commit => b"COMMIT",
            CommandKind::Abort => b"ABORT",
            CommandKind::Disconnect => b"DISCONNECT",
            CommandKind::Message => b"MESSAGE",
            CommandKind::Receipt => b"RECEIPT",
            CommandKind::Error => b"ERROR",
        }
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        match bytes {
            b"CONNECT" => Some(CommandKind::Connect),
            b"STOP" => Some(CommandKind::Stop),
            b"CONNECTED" => Some(CommandKind::Connected),
            b"SEND" => Some(CommandKind::Send),
            b"SUBSCRIBE" => Some(CommandKind::Subscribe),
            b"UNSUBSCRIBE" => Some(CommandKind::Unsubscribe),
            b"ACK" => Some(CommandKind::Ack),
            b"NACK" => Some(CommandKind::Nack),
            b"BEGIN" => Some(CommandKind::Begin),
            b"COMMIT" => Some(CommandKind::Commit),
            b"ABORT" => Some(CommandKind::Abort),
            b"DISCONNECT" => Some(CommandKind::Disconnect),
            b"MESSAGE" => Some(CommandKind::Message),
            b"RECEIPT" => Some(CommandKind::Receipt),
            b"ERROR" => Some(CommandKind::Error),
            _ => None,
        }
    }
}

impl Frame {
    pub fn new(command: CommandKind) -> Self {
        Self { command, headers: HashMap::new(), payload: Bytes::new() }
    }

    pub fn with_header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }

    pub fn with_payload(mut self, payload: Bytes) -> Self {
        self.payload = payload;
        self
    }

    pub fn parse(buf: &mut Cursor<&Bytes>) -> Result<Self, Error> {
        let command = CommandKind::from_bytes(Self::read_line(buf)?).ok_or(Error::new(
            ErrorKind::InvalidCommandError, "",
        ))?;
        let mut headers = HashMap::new();
        loop {
            let header = Self::read_line(buf)?;
            if header.len() == 0 {
                break;
            }
            let colon_index = header.iter().position(|n| *n == b':').ok_or(Error::new(ErrorKind::InvalidFrameFormat, ""))?;
            let name = std::str::from_utf8(&header[..colon_index]).map_err(|err| Error::new(
                ErrorKind::InvalidCommandError, format!("incorrect header format, err: {err}"),
            ))?;
            let value = std::str::from_utf8(&header[colon_index+1..]).map_err(|err| Error::new(
                ErrorKind::InvalidCommandError, format!("incorrect header format, err: {err}"),
            ))?;
            headers.insert(name.to_owned(), value.to_owned());
        }

        let payload = if let Some(content_length_raw) = headers.get("content-length") {
            let content_length = content_length_raw.parse::<u64>()
                .map_err(|err| Error::new(ErrorKind::InvalidCommandError, format!("could not parse content length, err: {err}")))?;
            let raw_payload = Self::read(buf, content_length)?;
            Bytes::from(raw_payload.to_vec())
        } else {
            Bytes::new()
        };

        Ok(Frame{
            command,
            headers: headers,
            payload,
        })
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut cap: usize;
        cap = self.command.as_bytes().len();
        cap += self.headers.len();
        for (key, value) in &self.headers {
            cap += key.as_bytes().len() + value.as_bytes().len();
        }
        cap += self.payload.len();

        let mut data = BytesMut::with_capacity(cap);
        data.put_slice(self.command.as_bytes());
        data.put_u8(b'\n');
        for (key, value) in &self.headers {
            data.put_slice(key.as_bytes());
            data.put_u8(b':');
            data.put_slice(value.as_bytes());
            data.put_u8(b'\n');
        }
        data.put_u8(b'\n');
        data.put_slice(&self.payload);
        data.put_u8(b'\0');
        data.freeze()
    }

    pub fn subscription(&self) -> Option<String> {
        self.headers.get("subscription").cloned()
    }

    pub fn receipt_id(&self) -> Result<Option<u32>, Error> {
        self.headers.get("receipt-id")
            .map(|id| id.parse::<u32>().map_err(|e| Error::new(ErrorKind::ParseError, format!("could not parse receipt id: {id}"))))
            .transpose()
    }

    fn read_line<'a>(buf: &mut Cursor<&'a Bytes>) -> Result<&'a [u8], Error> {
        if !buf.has_remaining() {
            return Err(Error::new(ErrorKind::FrameError, "incomplete"));
        }

        let start = buf.position() as usize;

        let new_line_index = buf.get_ref()[start..]
            .iter()
            .position(|c| *c == b'\n')
            .map(|pos| start + pos);
        if new_line_index.is_none() {
            return Err(Error::new(ErrorKind::FrameError, "cannot get new line"));
        };
        let new_line_index = new_line_index.unwrap();
            // .ok_or(Error::new(ErrorKind::FrameError, "cannot get new line"))?;

        let mut line = &buf.get_ref()[start..new_line_index + 1];
        if line.ends_with(b"\r\n") {
            line = &line[..line.len() - 2];
        } else if line.ends_with(b"\n") {
            line = &line[..line.len() - 1];
        }
        buf.set_position((new_line_index + 1) as u64);
        Ok(line)
    }

    fn read<'a>(buf: &mut Cursor<&'a Bytes>, n: u64) -> Result<&'a [u8], Error> {
        if !buf.has_remaining() {
            return Err(Error::new(ErrorKind::FrameError, "incomplete"));
        }

        let start = buf.position() as usize;
        let end = buf
            .get_ref()
            .iter()
            .position(|n| *n == b'\0')
            .ok_or(Error::new(
                ErrorKind::InvalidFrameFormat, "no trailing nil",
            ))?;
        if end > start + n as usize {
            return Err(Error::new(ErrorKind::FrameError, "incorrect number of bytes"));
        }
        buf.set_position(end as u64 + 1);
        // remove trailing nil
        Ok(&buf.get_ref()[start..end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_frame_with_headers_and_body() {
        let data = Bytes::from_static(b"SEND\nheader1:value1\nheader2:value2\ncontent-length:14\n\nHello, STOMP!\0");
        let mut cursor = Cursor::new(&data);
    
        let frame = Frame::parse(&mut cursor).expect("Failed to parse frame");
    
        assert_eq!(frame.command, CommandKind::Send);
        assert_eq!(frame.headers.get("header1").unwrap(), "value1");
        assert_eq!(frame.headers.get("header2").unwrap(), "value2");
        assert_eq!(frame.payload, Bytes::from("Hello, STOMP!"));
    }

    #[test]
    fn test_parse_frame_without_body() {
        let data = Bytes::from_static(b"SUBSCRIBE\nid:12345\ndestination:/queue/test\n\n");
        let mut cursor = Cursor::new(&data);

        let frame = Frame::parse(&mut cursor).expect("Failed to parse frame");

        assert_eq!(frame.command, CommandKind::Subscribe);
        assert_eq!(frame.headers.get("id").unwrap(), "12345");
        assert_eq!(frame.headers.get("destination").unwrap(), "/queue/test");
        assert!(frame.payload.is_empty());
    }

    #[test]
    fn test_parse_frame_with_only_command() {
        let data = Bytes::from_static(b"DISCONNECT\n\n");
        let mut cursor = Cursor::new(&data);

        let frame = Frame::parse(&mut cursor).expect("Failed to parse frame");

        assert_eq!(frame.command, CommandKind::Disconnect);
        assert!(frame.headers.is_empty());
        assert!(frame.payload.is_empty());
    }

    #[test]
    fn test_parse_frame_with_content_length_but_long_body() {
        let data = Bytes::from_static(b"MESSAGE\ncontent-length:5\n\nHello, STOMP!\0");
        let mut cursor = Cursor::new(&data);

        let result = Frame::parse(&mut cursor);
        assert!(result.is_err(), "Expected error for long body");
    }

    #[test]
    fn test_parse_frame_with_unknown_command() {
        let data = Bytes::from_static(b"UNKNOWN_COMMAND\nheader:value\n\n");
        let mut cursor = Cursor::new(&data);

        let result = Frame::parse(&mut cursor);
        assert!(result.is_err(), "Expected error for unknown command");
    }

    #[test]
    fn test_parse_frame_with_invalid_header() {
        let data = Bytes::from_static(b"SEND\ninvalid-header\n\nHello, STOMP!\0");
        let mut cursor = Cursor::new(&data);

        let result = Frame::parse(&mut cursor);
        assert!(result.is_err(), "Expected error for invalid header format");
    }

    #[test]
    fn test_parse_frame_without_double_newline() {
        let data = Bytes::from_static(b"SEND\nheader:value\nHello, STOMP!");
        let mut cursor = Cursor::new(&data);

        let result = Frame::parse(&mut cursor);
        assert!(result.is_err(), "Expected error for missing newline");
    }

    #[test]
    fn test_to_bytes_with_only_command() {
        let frame = Frame {
            command: CommandKind::Send,
            headers: HashMap::new(),
            payload: Bytes::new(),
        };

        let result = frame.to_bytes();
        assert_eq!(result, Bytes::from_static(b"SEND\n\n\0"));
    }

    #[test]
    fn test_to_bytes_with_headers() {
        let mut headers = HashMap::new();
        headers.insert("destination".to_string(), "queue/test".to_string());
        headers.insert("id".to_string(), "sub-1".to_string());

        let frame = Frame {
            command: CommandKind::Subscribe,
            headers,
            payload: Bytes::new(),
        };

        let result = frame.to_bytes();
        
        let expected = Bytes::from_static(b"SUBSCRIBE\ndestination:queue/test\nid:sub-1\n\n\0");
        assert_eq!(result, expected);
    }

    #[test]
    fn test_to_bytes_with_payload() {
        let frame = Frame {
            command: CommandKind::Message,
            headers: HashMap::new(),
            payload: Bytes::from_static(b"Hello, STOMP!"),
        };

        let result = frame.to_bytes();
        let expected = Bytes::from_static(b"MESSAGE\n\nHello, STOMP!\0");
        assert_eq!(result, expected);
    }

    #[test]
    fn test_to_bytes_with_headers_and_payload() {
        let mut headers = HashMap::new();
        headers.insert("destination".to_string(), "queue/test".to_string());

        let frame = Frame {
            command: CommandKind::Send,
            headers,
            payload: Bytes::from_static(b"Test Message"),
        };

        let result = frame.to_bytes();
        
        let expected = Bytes::from_static(b"SEND\ndestination:queue/test\n\nTest Message\0");
        assert_eq!(result, expected);
    }
}
