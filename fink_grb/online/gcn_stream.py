from gcn_kafka import Consumer
from subscribe import fermi_subscribe, swift_subscribe, icecube_subscribe, integral_subscribe

import voeventparse as vp
import io
import os
import configparser


if __name__ == "__main__":

    # read the config file
    config = configparser.ConfigParser(os.environ)

    # Connect as a consumer.
    # Warning: don't share the client secret with others.
    consumer = Consumer(client_id=config["CLIENT"]["id"],
                        client_secret=config["CLIENT"]["secret"])

    # Subscribe to topics and receive alerts
    consumer.subscribe(fermi_subscribe() + swift_subscribe() + icecube_subscribe() + integral_subscribe())

    i = 54
    while True:
        for message in consumer.consume():
            value = message.value()
            print(value)
            decode = io.BytesIO(value).read().decode("UTF-8")

            with open("voevent_database/voevent_number={}.xml".format(i), 'w') as f:
                f.write(decode)
            i += 1

        print()
        print("-------")
        print()