class BinnacleError(Exception):
    """Business error visible to clients, part of the observable contract."""


class TopicAlreadyExists(BinnacleError):
    pass


class UnknownTopic(BinnacleError):
    pass


class PartitionOutOfRange(BinnacleError):
    pass


class InvalidOffset(BinnacleError):
    pass
