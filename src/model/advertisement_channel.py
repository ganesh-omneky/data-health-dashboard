import enum


class AdvertisementChannel(enum.Enum):
    UNKNOWN = 0
    FACEBOOK = 1
    GOOGLE = 2
    TIKTOK = 3
    LINKEDIN = 4
    TWITTER = 5
    SNAPCHAT = 6
    REDDIT = 7
    OMNICHANNEL = 100

    @classmethod
    def get_channel_for_name(cls, channel_name: str):
        mapper = {
            "facebook": cls.FACEBOOK,
            "google": cls.GOOGLE,
            "tiktok": cls.TIKTOK,
            "linkedin": cls.LINKEDIN,
            "twitter": cls.TWITTER,
            "snapchat": cls.SNAPCHAT,
            "reddit": cls.REDDIT,
            "omnichannel": cls.OMNICHANNEL,
        }

        return mapper.get(channel_name.lower(), cls.UNKNOWN)
