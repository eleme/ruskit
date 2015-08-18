import argparse
import collections
import redis
import socket

# [[67, 76, [b'127.0.0.1', 7003], [b'127.0.0.1', 7000]],
#  [1619, 5961, [b'127.0.0.1', 7003], [b'127.0.0.1', 7000]],
#  [7202, 7233, [b'127.0.0.1', 7003], [b'127.0.0.1', 7000]],
#  [10923, 11421, [b'127.0.0.1', 7003], [b'127.0.0.1', 7000]],
#  [12662, 12693, [b'127.0.0.1', 7003], [b'127.0.0.1', 7000]],
#  [7385, 8163, [b'127.0.0.1', 7002], [b'127.0.0.1', 7005]],
#  [11422, 12661, [b'127.0.0.1', 7002], [b'127.0.0.1', 7005]],
#  [12723, 16383, [b'127.0.0.1', 7002], [b'127.0.0.1', 7005]],
#  [0, 66, [b'127.0.0.1', 7004], [b'127.0.0.1', 7001]],
#  [77, 1618, [b'127.0.0.1', 7004], [b'127.0.0.1', 7001]],
#  [5962, 7201, [b'127.0.0.1', 7004], [b'127.0.0.1', 7001]],
#  [7234, 7384, [b'127.0.0.1', 7004], [b'127.0.0.1', 7001]],
#  [8164, 10922, [b'127.0.0.1', 7004], [b'127.0.0.1', 7001]],
#  [12694, 12722, [b'127.0.0.1', 7004], [b'127.0.0.1', 7001]]]

def ip(hostname):
    return socket.gethostbyname(hostname)


class Slots(object):
    def __init__(self, host, port):
        self.cli = redis.Redis(host, port)

        self.sock = ip(host), port

    def get_slots(self):
        slot_map = {}
        slots = self.cli.execute_command("cluster slots")
        for slot in slots:
            master = ip(slot[2][0]), slot[2][1]
            if master not in slot_map:
                slot_map[master] = {"slots": [], "slaves": set()}

            slot_map[master]["slots"].append((slot[0], slot[1] + 1))

            if len(slot) > 3:
                slot_map[master]["slaves"].add(tuple(slot[3]))

        return slot_map

    def has_slots(self):
        return self.sock in self.get_slots()

    def del_slots(self, slots):
        self.cli.execute_command("cluster delslots {}".format(
            ' '.join(str(i) for i in slots)))

    def clear(self):
        slot_map = self.get_slots()
        if not self.sock in slot_map:
            return

        slots = slot_map[self.sock]
        for i in slots["slots"]:
            self.del_slots(range(*i))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    parser.add_argument("-s", "--show-slots", action="store_true")
    args = parser.parse_args()
    s = Slots(args.host, args.port)
    if args.show_slots:
        print(s.get_slots())
    else:
        s.clear()
        assert not s.has_slots()



if __name__ == "__main__":
    main()
