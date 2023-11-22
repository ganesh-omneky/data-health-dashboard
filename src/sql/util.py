from typing import List, Dict

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.sql.engine import get_engine
from src.sql.tables import (
    Ads,
    Brands,
    ImageAsset,
    PlatformInfo,
    Platforms,
    VideoAsset,
)
from src.model import AdvertisementChannel, AccountDetails


def account_details_from_db(
    account_id: str, channel: AdvertisementChannel
) -> AccountDetails:
    sql_engine = get_engine()
    with Session(sql_engine) as session:
        platform_id_stmt = select(Platforms.id).where(
            Platforms.name == channel.name.lower()
        )
        platform_id_subq = platform_id_stmt.subquery()
        account_details_stmt = (
            select(
                PlatformInfo.brand_id,
                PlatformInfo.account_name,
                PlatformInfo.token1,
                PlatformInfo.token2,
                PlatformInfo.target_words,
            )
            .where(PlatformInfo.account_id == account_id)
            .where(PlatformInfo.deleted_at.is_(None))
            .where(PlatformInfo.platform_id.in_(platform_id_subq))
        )
        platform_info_row = session.execute(account_details_stmt).fetchone()
        account_details = AccountDetails(account_id, channel=channel)
        if platform_info_row:
            account_details.brand_id = platform_info_row.brand_id
            account_details.account_name = platform_info_row.account_name
            account_details.token1 = platform_info_row.token1
            account_details.token2 = platform_info_row.token2
            account_details.target_words = platform_info_row.target_words

        return account_details


def get_all_account_details_from_db(channel: AdvertisementChannel) -> AccountDetails:
    sql_engine = get_engine()
    with Session(sql_engine) as session:
        platform_id_stmt = select(Platforms.id).where(
            Platforms.name == channel.name.lower()
        )
        platform_id_subq = platform_id_stmt.subquery()
        active_brands_stmt = select(Brands.id).where(Brands.is_active == True)
        active_brands_subq = active_brands_stmt.subquery()
        account_details_stmt = (
            select(PlatformInfo)
            .where(PlatformInfo.deleted_at.is_(None))
            .where(PlatformInfo.platform_id.in_(platform_id_subq))
            .where(PlatformInfo.brand_id.in_(active_brands_subq))
        )

        account_details_rows = session.execute(account_details_stmt).fetchall()

        all_account_details = []
        for account_details_row in account_details_rows:
            account_details = account_details_row[0]
            all_account_details.append(
                AccountDetails(
                    account_id=account_details.account_id,
                    brand_id=account_details.brand_id,
                    channel=channel,
                    account_name=account_details.account_name,
                    token1=account_details.token1,
                    token2=account_details.token2,
                    target_words=account_details.target_words,
                )
            )

        return all_account_details


def filter_exists(filters: Dict[str, List[str]], filter_name: str) -> bool:
    return filter_name in filters and len(filters[filter_name]) > 0
