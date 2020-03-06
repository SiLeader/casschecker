import struct
import socket
import typing
import enum
import argparse


_HEADER_FORMAT = '!BBhBi'


class OpCode(enum.Enum):
    Startup = 0x01
    Register = 0x0b
    Error = 0x00
    Ready = 0x02
    Result = 0x08
    Query = 0x07


class Consistency(enum.Enum):
    Any = 0x0000
    One = 0x0001
    Two = 0x0002
    Three = 0x0003
    Quorun = 0x0004
    All = 0x0005
    LocalQuorum = 0x0006
    EachQuorum = 0x0007
    Serial = 0x0008
    LocalSerial = 0x0009
    LocalOne = 0x000A

class Payload:
    def __init__(self, version: int, flags: int, stream: int, opcode: OpCode, body: bytes):
        self.__version = version
        self.__flags = flags
        self.__strema = stream
        self.__opcode = opcode
        self.__body = body
        self.__length = len(body)

    def __bytes__(self):
        return struct.pack(
            _HEADER_FORMAT, self.__version, self.__flags, self.__strema, self.__opcode.value, self.__length) + self.__body

    @property
    def version(self):
        return self.__version

    @property
    def flags(self):
        return self.__flags

    @property
    def stream(self):
        return self.__strema

    @property
    def opcode(self):
        return self.__opcode

    @property
    def body(self):
        return self.__body

    @property
    def length(self):
        return self.__length

    def __str__(self):
        return '{}:\n  version: {}\n  flags: {}\n  stream: {}\n  opcode: {}\n  length: {}'.format(
            type(self).__name__,
            self.version,
            self.flags,
            self.stream,
            self.opcode,
            self.length
        )


class Request(Payload):
    __current_stream = 0x0000

    def __init__(self, opcode: OpCode, body: bytes):
        super(Request, self).__init__(0x04, 0x00, Request.__current_stream, opcode, body)
        Request.__current_stream += 1


class Response(Payload):
    def __str__(self):
        if self.opcode == OpCode.Error:
            additional = '\n  code: {}\n  message: {}'.format(
                hex(struct.unpack('!i', self.body[:4])[0]), self.body[6:].decode(encoding='utf-8')
            )
        else:
            additional = ''
        return super(Response, self).__str__() + additional


def byte_to_bytes(data: int) -> bytes:
    return struct.pack('!B', data)


def short_to_bytes(data: int) -> bytes:
    return struct.pack('!h', data)


def int_to_bytes(data: int) -> bytes:
    return struct.pack('!i', data)


def shortstr_to_bytes(data: str) -> bytes:
    data = data.encode(encoding='utf-8')
    return short_to_bytes(len(data)) + data


def longstr_to_bytes(data: str) -> bytes:
    data = data.encode(encoding='utf-8')
    return int_to_bytes(len(data)) + data


def consistency_to_bytes(data: Consistency) -> bytes:
    return short_to_bytes(data.value)


def strstrmap_to_bytes(data: typing.Dict[str, str]) -> bytes:
    return short_to_bytes(len(data)) + b''.join([shortstr_to_bytes(k) + shortstr_to_bytes(v) for k, v in data.items()])


def send_request(sock: socket.socket, payload: Request) -> Response:
    sock.send(bytes(payload))
    header = sock.recv(9)

    version, flags, stream, opecode, length = struct.unpack(_HEADER_FORMAT, header)
    opecode = OpCode(opecode)
    body = sock.recv(length)

    return Response(version, flags, stream, opecode, body)


def __send_request_with_message(sock: socket.socket, payload: Request):
    print(payload)
    print(send_request(sock, payload), end='\n\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('host')
    parser.add_argument('--port', default=9042)

    args = parser.parse_args()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((args.host, args.port))

        startup = Request(OpCode.Startup, strstrmap_to_bytes({
            'CQL_VERSION': '3.0.0'
        }))

        __send_request_with_message(sock, startup)

        local = Request(OpCode.Query, longstr_to_bytes('SELECT * FROM system.local') + consistency_to_bytes(Consistency.One) + byte_to_bytes(0))
        __send_request_with_message(sock, local)

        peer = Request(OpCode.Query, longstr_to_bytes('SELECT * FROM system.peers') + consistency_to_bytes(Consistency.One) + byte_to_bytes(0))
        __send_request_with_message(sock, peer)

        while True:
            query = input('CQL > ')
            if query.startswith('q'):
                break
            peer = Request(OpCode.Query, longstr_to_bytes(query) + consistency_to_bytes(
                Consistency.One) + byte_to_bytes(0))
            __send_request_with_message(sock, peer)


if __name__ == '__main__':
    main()
