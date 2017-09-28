import playground
import asyncio
import playground
import sys
from playground.network.packet import PacketType
from playground.network.packet.fieldtypes import UINT8, UINT16, UINT32, BUFFER, STRING
from asyncio.protocols import Protocol
from playground.asyncio_lib.testing import TestLoopEx
from playground.network.testing import MockTransportToStorageStream
from playground.network.testing import MockTransportToProtocol
import random
from playground.network.packet.fieldtypes.attributes import Optional
from playground.common import logging as p_logging


class PEEP(PacketType):

    DEFINITION_IDENTIFIER = "PEEP.Packet"
    DEFINITION_VERSION = "1.0"

    FIELDS = [
        ("type", UINT8),
        ("sequencenumber", UINT32({Optional: True})),
        ("checksum", UINT16),
        ("acknowledgement", UINT32({Optional: True})),
        ("data", BUFFER({Optional: True}))
    ]

class Client(Protocol):


    def __init__(self):
        self.transport = None
        self.state = 0

    def tcp_checksum(self, instance):
        # instead of concat 16-bit words, we use data that is a multiple of 16
        # (i.e. 576, the whole segment)
        all_text = str(instance.type) + str(instance.sequencenumber) + str(instance.acknowledgement) + str(instance.data)
        sum = 0
        for i in range((0), len(all_text) - 1, 2):
            # get unicode/byte values of operands
            first_operand = ord(all_text[i])
            second_operand = ord(all_text[i + 1]) << 8
            # add
            current_sum = first_operand + second_operand
            # add and wrap around
            sum = ((sum + current_sum) & 0xffff) + ((sum + current_sum) >> 16)
        return sum

    def connection_made(self, transport):
        self.transport = transport
        if self.state == 0:
            packet = PEEP()
            packet.type = 0
            packet.sequencenumber = random.randrange(1, 1000, 1)
            packet.acknowledgement = 0
            #packet.data = b'test'
            self.state += 1
            packet.checksum = self.tcp_checksum(packet)
            packs = packet.__serialize__()
            self.transport.write(packs)


    def data_received(self, data):
        self.deserializer = PacketType.Deserializer()
        self.deserializer.update(data)
        for packet in self.deserializer.nextPackets():
            if self.state == 1 and packet.type == 1:
                print ("\nSYN-ACK Received. Seqno=", packet.sequencenumber," Ackno=",packet.acknowledgement)
                Clientpacket = PEEP()
                Clientpacket.type = 2
                Clientpacket.sequencenumber = packet.acknowledgement
                Clientpacket.acknowledgement = packet.sequencenumber + 1
                self.state += 1
                Clientpacket.checksum = self.tcp_checksum(Clientpacket)
                clientpacketbytes = Clientpacket.__serialize__()
                self.transport.write(clientpacketbytes)

class initiate():

    def send_first_packet(self):
        self.loop = loop
        return Client()

if __name__ == "__main__":

    #p_logging.EnablePresetLogging(p_logging.PRESET_TEST)
    loop = asyncio.get_event_loop()
    loop.set_debug(enabled=True)
    client = initiate()
    coro = playground.getConnector().create_playground_connection(client.send_first_packet, '20174.1.1.1', 7344)
    loop.run_until_complete(coro)
    loop.run_forever()
    loop.close()
