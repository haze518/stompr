use std::borrow::Cow;

#[derive(Debug)]
pub struct Error {
    repr: Box<ErrorRepr>
}

#[derive(Debug)]
struct ErrorRepr {
    kind: ErrorKind,
    detail: Option<Cow<'static, str>>
}

#[derive(Debug)]
pub enum ErrorKind {
    TransportError,
    FrameError,
    ReceiptError,
    InvalidCommandError,
    InvalidFrameFormat,
    SendError,
    ParseError,
    TimeoutError,
    TransportConnectionError,
}

impl Error {
    pub fn new<D: Into<Cow<'static, str>>>(kind: ErrorKind, detail: D) -> Error {
        Error {
            repr: Box::new(ErrorRepr { kind, detail: Some(detail.into()) })
        }
    }
}

impl ToString for Error {
    fn to_string(&self) -> String {
        match &self.repr.detail {
            Some(detail) => format!("{:?}: {}", self.repr.kind, detail),
            None => format!("{:?}", self.repr.kind),
        }
    }
}
