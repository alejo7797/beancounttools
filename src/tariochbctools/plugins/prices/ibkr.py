from datetime import datetime
from os import environ
from time import sleep
from typing import override

from beancount.core.number import D
from beanprice import source  # pyright: ignore[reportMissingTypeStubs]
from dateutil import tz
from ibflex import FlexQueryResponse, OpenPosition, client, parser


class Source(source.Source):
    def _make_query(self) -> FlexQueryResponse:
        token: str = environ["IBKR_TOKEN"]
        queryId: str = environ["IBKR_QUERY_ID"]

        try:
            response = client.download(token, queryId)
        except client.ResponseCodeError as e:
            if e.code == "1018":
                sleep(10)
                response = client.download(token, queryId)
            else:
                raise e

        return parser.parse(response)  # pyright: ignore[reportUnknownMemberType]

    def _handle_position(
        self, position: OpenPosition, ticker: str, time: datetime | None = None
    ) -> tuple[bool, source.SourcePrice | None]:
        if (
            position.symbol is None
            or position.reportDate is None
            or position.currency is None
            or position.markPrice is None
        ):
            raise RuntimeError(
                "Please adjust your FlexQuery to include the needed fields."
            )

        symbol = position.symbol.rstrip("z")
        symbol, _, _ = symbol.partition(".")
        if symbol == ticker and (time is None or time.date() == position.reportDate):
            price = D(position.markPrice)
            timezone = tz.gettz("Europe/Amsterdam")
            time = datetime.combine(
                position.reportDate, datetime.min.time()
            ).astimezone(timezone)

            return True, source.SourcePrice(price, time, position.currency)

        return False, None

    def _get_price(
        self, ticker: str, time: datetime | None = None
    ) -> source.SourcePrice | None:
        response = self._make_query()
        for statement in response.FlexStatements[::-1]:
            for position in statement.OpenPositions:
                has_price, price = self._handle_position(position, ticker, time)
                if has_price:
                    return price

    @override
    def get_latest_price(self, ticker: str) -> source.SourcePrice | None:
        return self._get_price(ticker)

    @override
    def get_historical_price(
        self, ticker: str, time: datetime
    ) -> source.SourcePrice | None:
        return self._get_price(ticker, time)
