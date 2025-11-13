#  N
# O E
#  S
DIRECTIONS = [
    "N",
    "NE",
    "E",
    "SE",
    "S",
    "SO",
    "O",
    "NO",
]


def encode_msg(obj):
    temp = obj["temperatura"]
    humid = obj["humedad"]
    wind = obj["direccion_viento"]

    wInt = [i for (i, x) in enumerate(DIRECTIONS) if x == wind][0]
    print("{}->{:b}".format(wind, wInt << 21))
