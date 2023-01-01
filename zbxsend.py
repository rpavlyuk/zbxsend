import struct
import time
import socket
import logging
import json
from typing import List, Optional, Union


class Metric:
    def __init__(
        self,
        host: str,
        key: str,
        value: Union[str, int, float],
        clock: Optional[int] = None,
        ns: Optional[int] = None,
    ):
        self.host = host
        self.key = key
        self.value = value
        self.clock = clock
        self.ns = ns

    def __repr__(self):
        if self.clock is None:
            return f"Metric({self.host}, {self.key}, {self.value})"
        return f"Metric({self.host}, {self.key}, {self.value}, {self.clock}, {self.ns})"


def send_to_zabbix(
    metrics: List[Metric],
    zabbix_host: str = "127.0.0.1",
    zabbix_port: int = 10051,
    timeout: int = 15,
) -> bool:
    """Send set of metrics to Zabbix server.""" 
    metrics_data = []
    for m in metrics:
        if not m.clock:
            i = time.time_ns()
            clock = i // 1000000000
            ns = i % 1000000000
        else:
            clock = m.clock
            ns = m.ns
        metrics_data.append(
            {
                "host": m.host,
                "key": m.key,
                "value": str(m.value),
                "clock": clock,
                "ns": ns,
            }
        )
    json_data = json.dumps(
        {"request": "sender data", "data": metrics_data},
        ensure_ascii=False,
        separators=(",", ":"),  # Remove spaces, to emulate zabbix_sender behaviour
    )
    data_len = struct.pack('<Q', len(json_data))
    packet = b'ZBXD\1' + data_len + json_data.encode()
    try:
        zabbix = socket.socket()
        zabbix.connect((zabbix_host, zabbix_port))
        zabbix.settimeout(timeout)
        # send metrics to zabbix
        zabbix.sendall(packet)
        # get response header from zabbix
        resp_hdr = _recv_all(zabbix, 13)
        if not resp_hdr.startswith(b'ZBXD\1') or len(resp_hdr) != 13:
            logger.error('Wrong zabbix response')
            return False
        resp_body_len = struct.unpack('<Q', resp_hdr[5:])[0]
        # get response body from zabbix
        resp_body = zabbix.recv(resp_body_len)
        resp = json.loads(resp_body)
        logger.debug('Got response from Zabbix: {}'.format(str(resp)))
        logger.info(resp.get('info'))
        if resp.get('response') != 'success':
            logger.error('Got error from Zabbix: {}'.format(str(resp)))
            return False
        return True
    except socket.timeout as e:
        logger.error("zabbix timeout: " + str(e))
        return False
    except Exception as e:
        logger.exception('Error while sending data to Zabbix: ' + str(e))
        return False
    finally:
        zabbix.close()


logger = logging.getLogger('zbxsender') 


def _recv_all(sock: socket.socket, count: int) -> bytes:
    buf = b''
    while len(buf) < count:
        chunk = sock.recv(count - len(buf))
        if not chunk:
            return buf
        buf += chunk
    return buf


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    send_to_zabbix([Metric('localhost', 'bucks_earned', 99999)], 'localhost', 10051)
