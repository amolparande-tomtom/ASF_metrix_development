import enum


class MnrServer(enum.Enum):
    EUR_SO_NAM = "postgresql://caprod-cpp-pgmnr-005.flatns.net/mnr?user=mnr_ro&password=mnr_ro"
    LAM_MEA_OCE_SEA = "postgresql://caprod-cpp-pgmnr-006.flatns.net/mnr?user=mnr_ro&password=mnr_ro"
