from sqlalchemy import String, Text, Date, ForeignKey, DateTime, JSON
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped, mapped_column
import datetime


class Base(DeclarativeBase):
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class Platforms(Base):
    __tablename__ = "platforms"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255))

    def __repr__(self):
        return f"Platform(id={self.id}, name={self.name})"


class Companies(Base):
    __tablename__ = "companies"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    email: Mapped[str] = mapped_column(String(255))
    website: Mapped[str] = mapped_column(String(255))
    stripe_id: Mapped[str] = mapped_column(String(255))
    card_brand: Mapped[str] = mapped_column(String(255))
    card_last_four: Mapped[int] = mapped_column()
    trial_ends_at: Mapped[datetime.date] = mapped_column()
    created_at: Mapped[datetime.date] = mapped_column()
    updated_at: Mapped[datetime.date] = mapped_column()

    def __repr__(self):
        return f"Company(id={self.id}, name={self.name}, email={self.email})"


class Brands(Base):
    __tablename__ = "brands"
    id: Mapped[int] = mapped_column(primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"))
    name: Mapped[str] = mapped_column(String(100))
    approve_mail: Mapped[str] = mapped_column(String(65535))
    is_active: Mapped[int] = mapped_column()
    locale: Mapped[str] = mapped_column(String(5))
    currency: Mapped[str] = mapped_column(String(5))
    logo_date: Mapped[datetime.date] = mapped_column()
    logo: Mapped[str] = mapped_column(String(255))

    def __repr__(self):
        return f"Brand(id={self.id}, name={self.name}, is_active={self.is_active})"


class PlatformInfo(Base):
    __tablename__ = "platform_info"
    id: Mapped[int] = mapped_column(primary_key=True)
    platform_id: Mapped[int] = mapped_column(ForeignKey("platforms.id"))
    brand_id: Mapped[int] = mapped_column(ForeignKey("brands.id"))
    account_id: Mapped[str] = mapped_column(String(255))
    account_name: Mapped[str] = mapped_column(String(255))
    token1: Mapped[str] = mapped_column(String(255))
    token2: Mapped[str] = mapped_column(String(255))
    page_id: Mapped[str] = mapped_column(String(255))
    target_words: Mapped[str] = mapped_column(String(65535))
    deleted_at: Mapped[datetime.date] = mapped_column()

    def __repr__(self):
        return (
            f"PlatformInfo(id={self.id}, "
            f"platform_id={self.platform_id}, "
            f"account_id={self.account_id}, account_name={self.account_name}"
        )


class AdCreatives(Base):
    __tablename__ = "ad_creatives"
    id: Mapped[int] = mapped_column(primary_key=True)
    platform_info_id: Mapped[int] = mapped_column(ForeignKey("platform_info.id"))
    platform_ad_creative_id: Mapped[str] = mapped_column(String(255))
    name: Mapped[str] = mapped_column(String(255))
    cta_type: Mapped[str] = mapped_column(String(255))
    image_hash: Mapped[str] = mapped_column(String(255))
    image_url: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(255))
    thumbnail_url: Mapped[str] = mapped_column(String(255))
    title: Mapped[str] = mapped_column(String(255))
    video_id: Mapped[str] = mapped_column(String(255))
    object_type: Mapped[str] = mapped_column(String(255))
    asset_feed_spec_json: Mapped[JSON] = mapped_column(JSON(65535))
    object_story_spec_json: Mapped[JSON] = mapped_column(JSON(65535))

    def __repr__(self):
        return (
            f"AdCreative(id={self.id}, "
            f"platform_info_id={self.platform_info_id}, "
            f"brand_id={self.brand_id}, "
            f"account_id={self.account_id}, "
            f"platform_ad_creative_id={self.platform_ad_creative_id}, "
            f"ad_creative_name={self.ad_creative_name}"
        )


class Campaigns(Base):
    __tablename__ = "campaigns"
    id: Mapped[int] = mapped_column(primary_key=True)
    platform_info_id: Mapped[int] = mapped_column(ForeignKey("platform_info.id"))
    platform_campaign_id: Mapped[str] = mapped_column(String(100))
    name: Mapped[str] = mapped_column(String(30))
    objective: Mapped[str] = mapped_column(String(20))
    status: Mapped[str] = mapped_column(String(25))

    def __repr__(self):
        return (
            f"Campaign(id={self.id}, platform_campaign_id={self.platform_campaign_id}, name={self.name},"
            f" platform_id={self.platform_info_id})"
        )


class AdGroups(Base):
    __tablename__ = "ad_groups"
    id: Mapped[int] = mapped_column(primary_key=True)
    platform_info_id: Mapped[int] = mapped_column(ForeignKey("platform_info.id"))
    campaign_id: Mapped[int] = mapped_column(ForeignKey("campaigns.id"))
    platform_ad_group_id: Mapped[str] = mapped_column(String(100))
    name: Mapped[str] = mapped_column(String(200))
    objective: Mapped[str] = mapped_column(String(20))
    status: Mapped[str] = mapped_column(String(25))

    def __repr__(self):
        return (
            f"AdGroup(id={self.id}, platform_ad_group_id={self.platform_ad_group_id}, "
            f"name={self.name}, platform_info_id={self.platform_info_id})"
        )


class Ads(Base):
    __tablename__ = "ads"
    id: Mapped[int] = mapped_column(primary_key=True)
    platform_info_id: Mapped[int] = mapped_column(ForeignKey("platform_info.id"))
    ad_group_id: Mapped[int] = mapped_column(ForeignKey("ad_groups.id"))
    ad_creative_id: Mapped[int] = mapped_column(ForeignKey("ad_creatives.id"))
    platform_ad_id: Mapped[str] = mapped_column(String(100))
    ad_type: Mapped[str] = mapped_column(String(100))
    landing_page_url: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(20))
    cta: Mapped[str] = mapped_column(String(20))

    def __repr__(self):
        return f"Ad(id={self.id}, ad_type={self.ad_type}, platform_ad_id={self.platform_ad_id})"


class DailyInsights(Base):
    __tablename__ = "daily_insights"
    id: Mapped[int] = mapped_column(primary_key=True)
    platform_ad_id: Mapped[str] = mapped_column(String(100))
    platform_info_id: Mapped[int] = mapped_column(ForeignKey("platform_info.id"))
    date: Mapped[datetime.date] = mapped_column()
    spend: Mapped[str] = mapped_column(String(10))

    def __repr__(self):
        return (
            f"DailyInsights(id={self.id}, platform_ad_id={self.platform_ad_id}, date={self.date},"
            f" spend={self.spend})"
        )


class CampaignDailyInsights(Base):
    __tablename__ = "campaigns_daily_insights"
    id: Mapped[int] = mapped_column(primary_key=True)
    platform_campaign_id: Mapped[str] = mapped_column(String(100))
    platform_info_id: Mapped[int] = mapped_column(ForeignKey("platform_info.id"))
    date: Mapped[datetime.date] = mapped_column()
    spend: Mapped[str] = mapped_column(String(10))

    def __repr__(self):
        return (
            f"DailyInsights(id={self.id}, platform_ad_id={self.platform_campaign_id}, date={self.date},"
            f" spend={self.spend})"
        )


class NetworkInsights(Base):
    __tablename__ = "network_insights"
    id: Mapped[int] = mapped_column(primary_key=True)
    platform_ad_id: Mapped[str] = mapped_column(String(100))
    platform_info_id: Mapped[int] = mapped_column(ForeignKey("platform_info.id"))
    date: Mapped[datetime.date] = mapped_column()
    network: Mapped[str] = mapped_column(String(100))
    spend: Mapped[str] = mapped_column(String(10))

    def __repr__(self):
        return (
            f"NetworkInsights(id={self.id}, platform_ad_id={self.platform_ad_id}, date={self.date},"
            f" network={self.network}, spend={self.spend})"
        )


class ImageAsset(Base):
    __tablename__ = "image"
    id: Mapped[int] = mapped_column(primary_key=True)
    ad_id: Mapped[int] = mapped_column(ForeignKey("ads.id"))
    platform_asset_id: Mapped[str] = mapped_column(String(255))
    source: Mapped[str] = mapped_column(String(255))
    height: Mapped[int] = mapped_column()
    width: Mapped[int] = mapped_column()
    no_of_faces: Mapped[int] = mapped_column()
    clip_image_desc: Mapped[str] = mapped_column(Text)
    blip2_image_desc: Mapped[str] = mapped_column(Text)

    def __repr__(self):
        return (
            f"Image(id={self.id}, ad_id={self.ad_id}, source={self.source}, height={self.height}, "
            f"width={self.width}, no_of_faces={self.no_of_faces}, clip_image_desc={self.clip_image_desc})"
        )


class ImageAssetInsights(Base):
    __tablename__ = "image_asset_insights"
    id: Mapped[int] = mapped_column(primary_key=True)
    platform_info_id: Mapped[str] = mapped_column(ForeignKey("platform_info.id"))
    platform_asset_id: Mapped[str] = mapped_column(String(255))
    platform_ad_id: Mapped[str] = mapped_column(String(255))
    date: Mapped[datetime.date] = mapped_column(Date)
    spend: Mapped[str] = mapped_column(String(10))

    def __repr__(self):
        return (
            f"ImageAssetInsights(id={self.id}, date={self.date}, "
            f"spend={self.spend}, platform_ad_id={self.platform_ad_id})"
        )


class TextAsset(Base):
    __tablename__ = "text"
    id: Mapped[int] = mapped_column(primary_key=True)
    ad_id: Mapped[int] = mapped_column(ForeignKey("ads.id"))
    platform_asset_id: Mapped[str] = mapped_column(String(255))
    type: Mapped[str] = mapped_column(String(5))
    text: Mapped[str] = mapped_column(Text)
    sentiment: Mapped[str] = mapped_column(String(8))

    def __repr__(self):
        return (
            f"TextAsset(id={self.id}, ad_id={self.ad_id}, type={self.type}, text={self.text}, "
            f"sentiment={self.sentiment})"
        )


class TextAssetInsights(Base):
    __tablename__ = "text_asset_insights"
    id: Mapped[int] = mapped_column(primary_key=True)
    platform_ad_id: Mapped[str] = mapped_column(String(255))
    platform_asset_id: Mapped[str] = mapped_column(String(255))
    platform_info_id: Mapped[int] = mapped_column(ForeignKey("platform_info.id"))
    date: Mapped[datetime.date] = mapped_column(Date)
    spend: Mapped[str] = mapped_column(String(10))
    text_type: Mapped[str] = mapped_column(String(5))

    def __repr__(self):
        return (
            f"TextAssetInsights(id={self.id}, date={self.date}, " f"spend={self.spend})"
        )


class VideoAsset(Base):
    __tablename__ = "video"
    id: Mapped[int] = mapped_column(primary_key=True)
    ad_id: Mapped[int] = mapped_column(ForeignKey("ads.id"))
    platform_asset_id: Mapped[str] = mapped_column(String(255))
    source: Mapped[str] = mapped_column(String(255))
    duration: Mapped[int] = mapped_column()
    no_of_faces: Mapped[int] = mapped_column()
    is_audio_present: Mapped[bool] = mapped_column()
    height: Mapped[int] = mapped_column()
    width: Mapped[int] = mapped_column()

    def __repr__(self):
        return (
            f"Video(id={self.id}, ad_id={self.ad_id}, source={self.source}, duration={self.duration}, "
            f"no_of_faces={self.no_of_faces})"
        )


class VideoAssetInsights(Base):
    __tablename__ = "video_asset_insights"
    id: Mapped[int] = mapped_column(primary_key=True)
    platform_ad_id: Mapped[str] = mapped_column(String(255))
    platform_asset_id: Mapped[str] = mapped_column(String(255))
    platform_info_id: Mapped[int] = mapped_column(ForeignKey("platform_info.id"))
    date: Mapped[datetime.date] = mapped_column(Date)
    spend: Mapped[str] = mapped_column(String(10))

    def __repr__(self):
        return (
            f"VideoAssetInsights(id={self.id}, date={self.date}, "
            f"spend={self.spend})"
        )


class FailedJobs(Base):
    __tablename__ = "failed_jobs"
    job_id: Mapped[int] = mapped_column(primary_key=True)
    platform_id: Mapped[int] = mapped_column(ForeignKey("platforms.id"))
    account_id: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(45))
    time_of_failure: Mapped[datetime.datetime] = mapped_column(DateTime)
    time_of_last_run: Mapped[datetime.datetime] = mapped_column(DateTime)
    time_of_next_retry: Mapped[datetime.datetime] = mapped_column(DateTime)
    run_status: Mapped[str] = mapped_column(String(45))
    retry_count: Mapped[int] = mapped_column()

    def __repr__(self):
        return (
            f"FailedJobs(platform_id={self.platform_id}, "
            f"account_id={self.account_id}"
            f"time_of_failure={self.time_of_failure}, "
            f"time_of_last_run={self.time_of_last_run}, "
            f"time_of_next_run={self.time_of_next_retry}, "
            f"run_status={self.run_status}, "
            f"retry_count={self.retry_count})"
        )


class AdAccountMetricSettings(Base):
    __tablename__ = "ad_account_metric_settings"
    id: Mapped[int] = mapped_column(primary_key=True)
    platform_id: Mapped[int] = mapped_column(ForeignKey("platforms.id"))
    brand_id: Mapped[int] = mapped_column(ForeignKey("brands.id"))
    metrics: Mapped[str] = mapped_column(String(255))
    conversion_setting: Mapped[str] = mapped_column(String(25))

    def __repr__(self):
        return (
            f"AdAccountMetricSettings(platform_id={self.platform_id}, "
            f"brand_id={self.brand_id}, "
            f"metrics={self.metrics}, "
            f"conversion_setting={self.conversion_setting})"
        )


class Assets(Base):
    __tablename__ = "assets"
    id: Mapped[int] = mapped_column(primary_key=True)
    brand_id: Mapped[int] = mapped_column(ForeignKey("brands.id"))
    name: Mapped[str] = mapped_column(String(255))
    size: Mapped[str] = mapped_column(String(255))
    extension: Mapped[str] = mapped_column(String(255))
    type: Mapped[str] = mapped_column(String(255))
    url: Mapped[str] = mapped_column(String(255))
    path: Mapped[str] = mapped_column(String(255))
    thumbnail: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime)
    star: Mapped[bool] = mapped_column()

    def __repr__(self):
        return (
            f"Assets(brand_id={self.brand_id}, "
            f"name={self.name}, "
            f"size={self.size}, "
            f"extension={self.extension}, "
            f"type={self.type}, "
            f"url={self.url}, "
            f"path={self.path}, "
            f"thumbnail={self.thumbnail}, "
            f"created_at={self.created_at}, "
            f"updated_at={self.updated_at}, "
            f"star={self.star})"
        )
