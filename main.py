import argparse
from chord import Chord

def main():
    parser = argparse.ArgumentParser("Please enter the ip , port, bit_length , peer ip and the peer port of the connecting ring",add_help=True)
    parser.add_argument("--Ip", required=True)
    parser.add_argument("--Port", required=True)
    parser.add_argument("--BitsLength", required=True)
    parser.add_argument("--PeerIp", required=False, default=None)
    parser.add_argument("--PeerPort", required=False,default=None)
    args = parser.parse_args()
    ip = args.Ip
    port = int(args.Port)
    bit_length = int(args.BitsLength)
    peer_ip = None
    if args.PeerIp is not None:
        peer_ip  = args.PeerIp
    peer_port = None
    if args.PeerPort is not None:    
        peer_port = int(args.PeerPort)
    chord = Chord(ip, bit_length , port, peer_ip, peer_port)


if __name__ == "__main__":
    main()