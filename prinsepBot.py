#!/usr/bin/env python3
# ~~~~~==============   HOW TO RUN   ==============~~~~~
# 1) Configure things in CONFIGURATION section
# 2) Change permissions: chmod +x prinsepBot.py
# 3) Run in loop: while true; do ./prinsepBot.py --test prod-like; sleep 1; done

import argparse
from collections import deque
from enum import Enum
from re import S
import time
import socket
import json

# ~~~~~============== CONFIGURATION  ==============~~~~~
# Replace "REPLACEME" with your team name!
team_name = "PRINSEPSTREET"


# ~~~~~============== HELPER FUNCTIONS  ==============~~~~~

BUY = "BUY"
SELL = "SELL"


class ExchangeConnection:
    def __init__(self, args):
        self.message_timestamps = deque(maxlen=500)
        self.exchange_hostname = args.exchange_hostname
        self.port = args.port
        exchange_socket = self._connect(add_socket_timeout=args.add_socket_timeout)
        self.reader = exchange_socket.makefile("r", 1)
        self.writer = exchange_socket

        self._write_message({"type": "hello", "team": team_name.upper()})

    def read_message(self):
        """Read a single message from the exchange"""
        message = json.loads(self.reader.readline())
        return message

    def send_add_message(
        self, order_id: int, symbol: str, dir: str, price: int, size: int
    ):
        """Add a new order"""
        self._write_message(
            {
                "type": "add",
                "order_id": order_id,
                "symbol": symbol,
                "dir": dir,
                "price": price,
                "size": size,
            }
        )

    def send_convert_message(self, order_id: int, symbol: str, dir: str, size: int):
        """Convert between related symbols"""
        self._write_message(
            {
                "type": "convert",
                "order_id": order_id,
                "symbol": symbol,
                "dir": dir,
                "size": size,
            }
        )

    def send_cancel_message(self, order_id: int):
        """Cancel an existing order"""
        self._write_message({"type": "cancel", "order_id": order_id})

    def _connect(self, add_socket_timeout):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if add_socket_timeout:
            # Automatically raise an exception if no data has been recieved for
            # multiple seconds. This should not be enabled on an "empty" test
            # exchange.
            s.settimeout(5)
        s.connect((self.exchange_hostname, self.port))
        return s

    def _write_message(self, message):
        what_to_write = json.dumps(message)

        print("Wrote message", what_to_write)

        if not what_to_write.endswith("\n"):
            what_to_write = what_to_write + "\n"

        length_to_send = len(what_to_write)
        total_sent = 0
        while total_sent < length_to_send:
            sent_this_time = self.writer.send(
                what_to_write[total_sent:].encode("utf-8")
            )
            if sent_this_time == 0:
                raise Exception("Unable to send data to exchange")
            total_sent += sent_this_time

        now = time.time()
        self.message_timestamps.append(now)
        if len(
            self.message_timestamps
        ) == self.message_timestamps.maxlen and self.message_timestamps[0] > (now - 1):
            print(
                "WARNING: You are sending messages too frequently. The exchange will start ignoring your messages. Make sure you are not sending a message in response to every exchange message."
            )




BOND = "BOND"
VALBZ = "VALBZ"
VALE = "VALE"
GS = "GS"
MS = "MS"
WFC = "WFC"
XLF = "XLF"



class Message:

    def __init__(self, raw_message):
        self.message = raw_message

    def get_type(self):
        return self.message["type"]

    def get_symbol(self):
        return self.message["symbol"]

    def get_best_bid(self):
        return self.message["buy"][0][0]

    def get_best_ask(self):
        return self.message["sell"][0][0]

    def get_direction(self):
        return self.message["dir"]
    
    def get_order_id(self):
        return self.message["order_id"]

    def get_size(self):
        return self.message["size"]


class Algorithm:

    def __init__(self, exchange: ExchangeConnection):
        self.exchange = exchange
        self.cur_order_id = 0

        # integer to 
        self.conversions = {}


        self.positions = {
            BOND: 0,
            VALBZ: 0,
            VALE: 0,
            GS: 0,
            MS: 0,
            WFC: 0,
            XLF: 0,
        }

        self.latest_best_asks = {
            BOND: float("-inf"),
            VALBZ: float("-inf"),
            VALE: float("-inf"),
            GS: float("-inf"),
            MS: float("-inf"),
            WFC: float("-inf"),
            XLF: float("-inf"),
        }

        self.latest_best_bids = {
            BOND: float("inf"),
            VALBZ: float("inf"),
            VALE: float("inf"),
            GS: float("inf"),
            MS: float("inf"),
            WFC: float("inf"),
            XLF: float("inf"),
        }
        
        self.all_orders = {

        }

        self.orders_by_symbol = {
            BOND: {BUY: set(), SELL: set()},
            VALBZ: {BUY: set(), SELL: set()},
            VALE: {BUY: set(), SELL: set()},
            GS: {BUY: set(), SELL: set()},
            MS: {BUY: set(), SELL: set()},
            WFC: {BUY: set(), SELL: set()},
            XLF: {BUY: set(), SELL: set()},
        }

        self.sent_converted_vale = False


    def take_handshake_message(self, message):
        market = message["symbols"]

        for symbol_dict in market:
            self.positions[symbol_dict["symbol"]] = symbol_dict["position"]

    # track best qtys
    def remember_best(self, message_obj: Message):
        symbol = message_obj.get_symbol()
        if len(message_obj.message["buy"]) != 0:
            self.latest_best_bids[symbol] = message_obj.get_best_bid()

        
        if len(message_obj.message["sell"]) != 0:
            self.latest_best_asks[symbol] = message_obj.get_best_ask()


    def parse(self, message_obj: Message):
        if message_obj.get_type() == "hello":
            self.take_handshake_message(message_obj.message)
            return
        
        self.evaluate(message_obj)


    def place_order(self, symbol, dir, price, size):

        # send message
        self.exchange.send_add_message(self.cur_order_id, symbol, dir, price, size)

        self.all_orders[self.cur_order_id] = {
            "to_fill": size,
            "filled": 0
        }

        # rmb the id
        self.orders_by_symbol[symbol][dir].add(self.cur_order_id)

        self.cur_order_id += 1


    def handle_ack(self, message_obj: Message):

        order_id = message_obj.get_order_id()
        
        # if this is a conversion - right now hardcode VALE and VALBZ
        if order_id in self.conversions:

            d = self.conversions[order_id]

            if d["symbol"] == VALE:
                
                self.sent_converted_vale = False  # reset

                if d["side"] == BUY:
                    self.positions[VALE] += d["size"]
                    self.positions[VALBZ] -= d["size"]
                if d["side"] == SELL:
                    self.positions[VALE] -= d["size"]
                    self.positions[VALBZ] += d["size"]


            if d["symbol"] == XLF:

                if d["side"] == BUY:
                    self.positions[XLF] += d["size"]
                    self.positions[BOND] -= d["size"] * 0.3
                    self.positions[GS] -= d["size"] * 0.2
                    self.positions[MS] -= d["size"] * 0.3
                    self.positions[WFC] -= d["size"] * 0.2
                
                if d["side"] == SELL:  # sell xlf for underlying
                    self.positions[XLF] -= d["size"]
                    self.positions[BOND] += d["size"] * 0.3
                    self.positions[GS] += d["size"] * 0.2
                    self.positions[MS] += d["size"] * 0.3
                    self.positions[WFC] += d["size"] * 0.2


    def add_fill(self, message_obj: Message):
        
        symbol = message_obj.get_symbol()
        order_id = message_obj.get_order_id()

        self.all_orders[order_id]["filled"] += message_obj.get_size()

        # track current position
        self.positions[symbol] += message_obj.message["size"] * (1 if message_obj.get_direction() == BUY else -1)


        # cleared all
        if self.all_orders[order_id]["filled"] == self.all_orders[order_id]["to_fill"]:

            # remove from set of integers
            if order_id in self.orders_by_symbol[symbol][message_obj.get_direction()]:
                self.orders_by_symbol[symbol][message_obj.get_direction()].remove(order_id)

        # algo
        if message_obj.get_symbol() == VALE:

            if message_obj.message["dir"] == BUY:

                # sell VALBZ at last ask
                self.place_order(VALBZ, SELL, self.latest_best_asks[VALBZ], 1)

            else:

                # buy VALBZ at last bid
                self.place_order(VALBZ, BUY, self.latest_best_bids[VALBZ], 1)



    def evaluate(self, message_obj: Message):

        if message_obj.get_type() == "book":

            self.remember_best(message_obj)

            symbol = message_obj.get_symbol()

            if symbol == VALE:
                self.vale_algo(message_obj)

            if symbol == BOND:
                self.bond_algo(message_obj)

            if symbol == XLF:
                self.xlf_algo(message_obj)
                

        if message_obj.get_type() == "fill":
            self.add_fill(message_obj)

        if message_obj.get_type == "ack":
            self.handle_ack(message_obj)

        self.independent()


    def xlf_algo(self, message_obj: Message):

        
        if len(message_obj.message["buy"]) != 0 and len(message_obj.message["sell"]) != 0:

            best_bid, best_ask = message_obj.get_best_bid(), message_obj.get_best_ask()

            # Implied bid Price XLF =  3 * 1000 + 2 * bid price GS + 3 * bid price MS + 2 * bid price WFC
            implied_bid_xlf = 3 * 1000 + 2 * self.latest_best_bids[GS] + 3 * self.latest_best_bids[MS] + 2 * self.latest_best_bids[WFC]

            # Implied ask Price XLF =  3 * 1000 + 2 * ask price GS + 3 * ask price MS + 2 * ask price WFC
            implied_ask_xlf = 3 * 1000 + 2 * self.latest_best_asks[GS] + 3 * self.latest_best_asks[MS] + 2 * self.latest_best_asks[WFC]


            if implied_ask_xlf + 20 < best_bid:

                # sell 20 xlf at bid price
                self.place_order(XLF, SELL, best_bid, 20)

                # buy all underlying at ask price
                # buy 6 BOND, 4 GS, 6 MS, 4 WFC at ask price
                self.place_order(BOND, BUY, self.latest_best_asks[BOND], 6)
                self.place_order(GS, BUY, self.latest_best_asks[GS], 4)
                self.place_order(MS, BUY, self.latest_best_asks[MS], 6)
                self.place_order(WFC, BUY, self.latest_best_asks[WFC], 4)

            elif implied_bid_xlf > best_ask + 20:

                # buy 20 xlf at ask price
                self.place_order(XLF, BUY, best_ask, 20)

                # sell all underlying at ask price
                # sell 6 BOND, 4 GS, 6 MS, 4 WFC at ask price
                self.place_order(BOND, SELL, self.latest_best_asks[BOND], 6)
                self.place_order(GS, SELL, self.latest_best_asks[GS], 4)
                self.place_order(MS, SELL, self.latest_best_asks[MS], 6)
                self.place_order(WFC, SELL, self.latest_best_asks[WFC], 4)

            elif implied_bid_xlf + 30 < best_ask:

                # buy all underlying at bid price
                # buy 3 BOND, 2 GS, 3 MS, 2 WFC at bid price
                self.place_order(BOND, BUY, self.latest_best_bids[BOND], 3)
                self.place_order(GS, BUY, self.latest_best_bids[GS], 2)
                self.place_order(MS, BUY, self.latest_best_bids[MS], 3)
                self.place_order(WFC, BUY, self.latest_best_bids[WFC], 2)

                # sell 10 xlf at ask price
                self.place_order(XLF, SELL, best_ask, 10)

            elif implied_ask_xlf > best_bid + 30:

                # buy 10 xlf at bid price
                self.place_order(XLF, BUY, best_bid, 10)

                # sell all underlying at ask price
                # sell 3 BOND, 2 GS, 3 MS, 2 WFC at ask price
                self.place_order(BOND, SELL, self.latest_best_asks[BOND], 3)
                self.place_order(GS, SELL, self.latest_best_asks[GS], 2)    
                self.place_order(MS, SELL, self.latest_best_asks[MS], 3)
                self.place_order(WFC, SELL, self.latest_best_asks[WFC], 2)

            
            # when cur pos of XLF == 100, convert 50 to underlying
            if self.positions[XLF] == 100:
                
                self.exchange.send_convert_message(
                    self.cur_order_id,
                    XLF,
                    SELL,
                    50
                )

                self.conversions[self.cur_order_id] = {"side": SELL, "size": 50, "symbol": XLF}

                self.cur_order_id += 1

            # when cur pos of XLF == -100, convert underlying to 50
            if self.positions[XLF] == -100:

                self.exchange.send_convert_message(
                    self.cur_order_id,
                    XLF,
                    BUY,
                    50
                )

                self.conversions[self.cur_order_id] = {"side": BUY, "size": 50, "symbol": XLF}

                self.cur_order_id += 1

                    
    
    def bond_algo(self, message_obj: Message):

        if len(message_obj.message["buy"]) != 0 and len(message_obj.message["sell"]) != 0:
            best_bid, best_ask = message_obj.get_best_bid(), message_obj.get_best_ask()

            spread = best_ask - best_bid
            X = spread - 2

            unfilled_buy_orders = self.orders_by_symbol[BOND][BUY]
            unfilled_sell_orders = self.orders_by_symbol[BOND][SELL]

            total_unfilled_lo = len(unfilled_buy_orders) + len(unfilled_sell_orders)

            if total_unfilled_lo < 20:

                BOND_INV = self.positions[BOND]

                if abs(BOND_INV) < 15:
                    if spread > 2 and best_bid < 1000 and best_ask > 1000:

                        # buy X at bid price + 1
                        self.place_order(BOND, BUY, best_bid + 1, X)

                         # sell X at ask price - 1
                        self.place_order(BOND, SELL, best_ask - 1, X)

                elif BOND_INV >= 15:
                    if spread > 2 and best_bid < 1000 and best_ask > 1000:

                        # buy X at bid price + 1
                        self.place_order(BOND, BUY, best_bid + 1, X)

                        # sell 2*X at ask price - 1
                        self.place_order(BOND, SELL, best_ask - 1, 2*X)

                elif BOND_INV <= -15:
                    if spread > 2 and best_bid < 1000 and best_ask > 1000:

                        # buy 2*X at bid price + 1
                        self.place_order(BOND, BUY, best_bid + 1, 2*X)

                        # sell X at ask price - 1
                        self.place_order(BOND, SELL, best_ask - 1, X)               

    
    def vale_algo(self, message_obj: Message):

        if len(message_obj.message["buy"]) != 0 and len(message_obj.message["sell"]) != 0:
            best_bid, best_ask = message_obj.get_best_bid(), message_obj.get_best_ask()

            if best_bid < self.latest_best_asks[VALBZ] - 3:

                # cancel all VALE sells
                for order_id in self.orders_by_symbol[VALE][SELL]:
                    self.exchange.send_cancel_message(order_id)

                self.orders_by_symbol[VALE][SELL] = set()

                # place BUY LO 1 at best_bid
                self.place_order(VALE, BUY, best_bid, 1)

            if best_bid > self.latest_best_asks[VALBZ] + 3:

                # cancel all VALE buys
                for order_id in self.orders_by_symbol[VALE][BUY]:
                    self.exchange.send_cancel_message(order_id)

                self.orders_by_symbol[VALE][BUY] = set()

                # place SELL LO 1 at best_ask
                self.place_order(VALE, SELL, best_ask, 1)
    

    def independent(self):

        if self.sent_converted_vale:
            return

        IVALE = round(self.positions[VALE])
        IVALBZ = round(self.positions[VALBZ])

        if IVALE == 10:

            amt = (IVALE - IVALBZ) // 2

            # convert amt vale to valbz
            self.exchange.send_convert_message(self.cur_order_id, VALE, SELL, amt)
            self.conversions[self.cur_order_id] = {"side": SELL, "size": amt, "symbol": VALE}
            self.cur_order_id += 1

        elif IVALE == -10:
            
            amt = (IVALBZ - IVALE) // 2
            self.exchange.send_convert_message(self.cur_order_id, VALE, BUY, amt)
            self.conversions[self.cur_order_id] = {"side": BUY, "size": amt, "symbol": VALE}
            self.cur_order_id += 1

        elif IVALBZ == 10:
            
            amt = (IVALBZ - IVALE) // 2
            self.exchange.send_convert_message(self.cur_order_id, VALE, BUY, amt)
            self.conversions[self.cur_order_id] = {"side": BUY, "size": amt, "symbol": VALE}
            self.cur_order_id += 1

        elif IVALBZ == -10:

            amt = (IVALE - IVALBZ) // 2
            # convert amt vale to valbz
            self.exchange.send_convert_message(self.cur_order_id, VALE, SELL, amt)
            self.conversions[self.cur_order_id] = {"side": SELL, "size": amt, "symbol": VALE}
            self.cur_order_id += 1

        self.sent_converted_vale = True

        # if round(self.positions[VALE]) == -10:

        #     # convert 5 valbz to vale
        #     self.exchange.send_convert_message(self.cur_order_id, VALE, BUY, 5)
        #     self.conversions[self.cur_order_id] = {"side": BUY, "size": 5, "symbol": VALE}
        #     self.cur_order_id += 1     



# ~~~~~============== MAIN LOOP ==============~~~~~


def main():
    args = parse_arguments()

    exchange = ExchangeConnection(args=args)

    # Store and print the "hello" message received from the exchange. This
    # contains useful information about your positions. Normally you start with
    # all positions at zero, but if you reconnect during a round, you might
    # have already bought/sold symbols and have non-zero positions.
    hello_message = exchange.read_message()
    print("First message from exchange:", hello_message)

    # Send an order for BOND at a good price, but it is low enough that it is
    # unlikely it will be traded against. Maybe there is a better price to
    # pick? Also, you will need to send more orders over time.
    # exchange.send_add_message(order_id=1, symbol="BOND", dir=BUY, price=990, size=1)

    # Set up some variables to track the bid and ask price of a symbol. Right
    # now this doesn't track much information, but it's enough to get a sense
    # of the VALE market.
    # vale_bid_price, vale_ask_price = None, None
    # vale_last_print_time = time.time()

    # Here is the main loop of the program. It will continue to read and
    # process messages in a loop until a "close" message is received. You
    # should write to code handle more types of messages (and not just print
    # the message). Feel free to modify any of the starter code below.
    #
    # Note: a common mistake people make is to call write_message() at least
    # once for every read_message() response.
    #
    # Every message sent to the exchange generates at least one response
    # message. Sending a message in response to every exchange message will
    # cause a feedback loop where your bot's messages will quickly be
    # rate-limited and ignored. Please, don't do that!

    algo = Algorithm(exchange=exchange)


    while True:
        message = exchange.read_message()
        print(message)
        print("Current positions:", algo.positions)
        message_obj = Message(message)

        # Some of the message types below happen infrequently and contain
        # important information to help you understand what your bot is doing,
        # so they are printed in full. We recommend not always printing every
        # message because it can be a lot of information to read. Instead, let
        # your code handle the messages and just print the information
        # important for you!

        algo.parse(message_obj)


        # if message_obj.get_type() == "close":
        #     print("The round has ended")
        #     break
        # elif message_obj.get_type() == "hello":
        #     print("The round has started")
        #     algo.take_handshake_message(message)
        # elif message_obj.get_type() == "error":
        #     print(message)
        # elif message_obj.get_type() == "reject":
        #     print(message)
        # elif message_obj.get_type() == "fill":

        #     algo.add_fill(message_obj)


        # elif message_obj.get_type() == "book":

        #     if message_obj.get_symbol() == "VALE":

        #         algo.remember_best(message_obj)


        #         vale_bid_price = message_obj.get_best_bid()
        #         vale_ask_price = message_obj.get_best_ask()

        #         now = time.time()

        #         if now > vale_last_print_time + 1:
        #             vale_last_print_time = now
        #             print(
        #                 {
        #                     "vale_bid_price": vale_bid_price,
        #                     "vale_ask_price": vale_ask_price,
        #                 }
        #             )


# ~~~~~============== PROVIDED CODE ==============~~~~~

# You probably don't need to edit anything below this line, but feel free to
# ask if you have any questions about what it is doing or how it works. If you
# do need to change anything below this line, please feel free to


def parse_arguments():
    test_exchange_port_offsets = {"prod-like": 0, "slower": 1, "empty": 2}

    parser = argparse.ArgumentParser(description="Trade on an ETC exchange!")
    exchange_address_group = parser.add_mutually_exclusive_group(required=True)
    exchange_address_group.add_argument(
        "--production", action="store_true", help="Connect to the production exchange."
    )
    exchange_address_group.add_argument(
        "--test",
        type=str,
        choices=test_exchange_port_offsets.keys(),
        help="Connect to a test exchange.",
    )

    # Connect to a specific host. This is only intended to be used for debugging.
    exchange_address_group.add_argument(
        "--specific-address", type=str, metavar="HOST:PORT", help=argparse.SUPPRESS
    )

    args = parser.parse_args()
    args.add_socket_timeout = True

    if args.production:
        args.exchange_hostname = "production"
        args.port = 25000
    elif args.test:
        args.exchange_hostname = "test-exch-" + team_name
        args.port = 25000 + test_exchange_port_offsets[args.test]
        if args.test == "empty":
            args.add_socket_timeout = False
    elif args.specific_address:
        args.exchange_hostname, port = args.specific_address.split(":")
        args.port = int(port)

    return args


if __name__ == "__main__":
    # Check that [team_name] has been updated.
    assert (
        team_name != "REPLACEME"
    ), "Please put your team name in the variable [team_name]."

    main()
