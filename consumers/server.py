"""Defines a Tornado Server that consumes Kafka Event data for display"""
import logging
import logging.config
import time
from pathlib import Path

import tornado.ioloop
import tornado.template
import tornado.web


# Import logging before models to ensure configuration is picked up
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")


from consumer import KafkaConsumer
from models import Lines, Weather
import topic_check
import topics


logger = logging.getLogger(__name__)
WEB_SERVER_PORT = 8888


class MainHandler(tornado.web.RequestHandler):
    """Serves the dashboard shell; the data arrives from StatusHandler"""

    template_dir = tornado.template.Loader(f"{Path(__file__).parents[0]}/templates")
    template = template_dir.load("status.html")

    def get(self):
        """Responds to get requests"""
        logging.debug("rendering and writing handler template")
        self.write(MainHandler.template.generate())


class StatusHandler(tornado.web.RequestHandler):
    """Serves the current state of the network as JSON"""

    def initialize(self, weather, lines):
        """Initializes the handler with required configuration"""
        self.weather = weather
        self.lines = lines

    def get(self):
        """Responds to get requests"""
        lines = self.lines.to_dict()
        stations = [station for line in lines for station in line["stations"]]
        arrivals = [
            arrival
            for station in stations
            for arrival in (station["dir_a"], station["dir_b"])
            if arrival is not None
        ]

        # The browser polls this endpoint, so a cached response would freeze
        # the board on whatever the first request happened to return.
        self.set_header("Cache-Control", "no-store")
        self.write(
            {
                "generated_at": time.time(),
                "weather": self.weather.to_dict(),
                "lines": lines,
                "totals": {
                    "stations": len(stations),
                    "trains": len(arrivals),
                    "entries": sum(line["entries"] for line in lines),
                    "broken_down": sum(
                        1 for arrival in arrivals if arrival["status"] == "broken down"
                    ),
                },
                # Normalising every turnstile bar against the busiest station in
                # the whole network keeps a full bar meaning the same thing in
                # all three columns.
                "max_entries": max(
                    (station["entries"] for station in stations), default=0
                ),
            }
        )


def run_server():
    """Runs the Tornado Server and begins Kafka consumption"""
    if topic_check.topic_exists(topics.TURNSTILE_SUMMARY) is False:
        logger.fatal(
            "Ensure that the KSQL Command has run successfully before running the web server!"
        )
        exit(1)

    if topic_check.topic_pattern_match("stations.table") is False:
        logger.fatal(
            "Ensure that Faust Streaming is running successfully before running the web server!"
        )
        exit(1)

    weather_model = Weather()
    lines = Lines()

    application = tornado.web.Application(
        [
            (r"/", MainHandler),
            (r"/api/status", StatusHandler, {"weather": weather_model, "lines": lines}),
        ],
        static_path=f"{Path(__file__).parents[0]}/static",
    )
    application.listen(WEB_SERVER_PORT)

    # Build kafka consumers. These are exact topic names: librdkafka only
    # treats a subscription as a regular expression when it starts with `^`.
    consumers = [
        KafkaConsumer(
            topics.WEATHER,
            weather_model.process_message,
            offset_earliest=True,
        ),
        KafkaConsumer(
            topics.STATIONS_TABLE,
            lines.process_message,
            offset_earliest=True,
            is_avro=False,
        ),
        KafkaConsumer(
            topics.ARRIVALS,
            lines.process_message,
            offset_earliest=True,
        ),
        KafkaConsumer(
            topics.TURNSTILE_SUMMARY,
            lines.process_message,
            offset_earliest=True,
            is_avro=False,
        ),
    ]

    try:
        logger.info(
            f"Open a web browser to http://localhost:{WEB_SERVER_PORT} to see the Transit Status Page"
        )
        for consumer in consumers:
            tornado.ioloop.IOLoop.current().spawn_callback(consumer.consume)

        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        logger.info("shutting down server")
        tornado.ioloop.IOLoop.current().stop()
        for consumer in consumers:
            consumer.close()


if __name__ == "__main__":
    run_server()
