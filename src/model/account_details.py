from typing import Optional
from src.model.advertisement_channel import AdvertisementChannel


class AccountDetails:
    def __init__(
        self,
        account_id: str,
        channel: AdvertisementChannel,
        brand_id: Optional[str] = None,
        account_name: Optional[str] = None,
        token1: Optional[str] = None,
        token2: Optional[str] = None,
        target_words: Optional[str] = None,
    ):
        self.brand_id = brand_id
        self.account_id = account_id
        self.account_name = account_name
        self.channel = channel
        self.token1 = token1
        self.token2 = token2
        self.target_words = target_words

        if account_name is None:
            self.description = f"({channel})[{self.account_id} | UNKNOWN]"
        else:
            self.description = f"({channel})[{self.account_id} | {account_name}]"
