from playground.network.packet import PacketType
from playground.network.packet.fieldtypes import STRING,INT, BOOL, UINT32, ListFieldType, UINT8, UINT16, BUFFER
from playground.network.packet.fieldtypes.attributes import Optional
import asyncio
import random
from playground.network.common.Protocol import StackingProtocol, StackingTransport, StackingProtocolFactory
import playground

#TCP Packet
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

class PEEPServerProtocol(asyncio.Protocol):
    serverstate = 0
    clientseq = 0
    serverseq = 0

    def __init__(self):
        self.deserializer = PacketType.Deserializer()
        self.transport = None

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
        #higherTransport = StackingTransport(self.transport)
        #self.higherProtocol().connection_made(higherTransport)

    def data_received(self, data):
        self.deserializer.update(data)
        for pkt in self.deserializer.nextPackets():
            # CheckSUM Check
            # SYN from client
            if pkt.type == 0 and self.serverstate == 0:
                self.serverstate += 1
                self.clientseq = pkt.sequencenumber
                if(pkt.checksum == self.tcp_checksum(pkt)):
                    print("\nSYN Received. Seq=",pkt.sequencenumber)
                    synack = PEEP()
                    synack.type = 1
                    synack.acknowledgement = pkt.sequencenumber + 1
                    synack.sequencenumber = random.randint(5000, 9999)
                    self.serverseq = synack.sequencenumber
                    synack.checksum = self.tcp_checksum(synack)
                    self.transport.write(synack.__serialize__())
                else:
                    print("Checksum error. Packet data corrupt.")
                    self.transport.close()
            elif pkt.type == 2 and self.serverstate == 1 and pkt.sequencenumber == self.clientseq + 1 and pkt.acknowledgement == self.serverseq + 1:
                # Data transmission can start
                print("\nACK Received. Seqno=",pkt.sequencenumber," Ackno=",pkt.acknowledgement)
                self.serverstate += 1
                if(pkt.checksum == self.tcp_checksum(pkt)):
                    print("\nTCP Connection successful. Client OK to send the data now.\n")
                else:
                    print("Corrupt ACK packet. Please check on client end.")
                self.higherProtocol().data_received(data)
            # Reset packet received
            elif pkt.Type == 5:
                print("Server received connection close from client. Closing socket.")


if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    conn = playground.getConnector().create_playground_server(lambda: PEEPServerProtocol() , 7344)
    server = loop.run_until_complete(conn)
    print("Echo Server Started at {}".format(server.sockets[0].gethostname()))
    print('\nPress Ctrl+C to terminate the process')
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    loop.close()
