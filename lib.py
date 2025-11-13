import math

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


def float_to_14bit_no_sign(value):
    """
    Converts a normal floating-point number to 14-bit custom format.
    Format: 5-bit exponent + 9-bit mantissa (no sign bit)

    Args:
        value: A positive Python float

    Returns:
        int: 14-bit integer representation
    """

    # Handle zero
    if value == 0:
        return 0

    # Take absolute value
    value = abs(value)

    # Normalize: express as mantissa × 2^exponent where mantissa ∈ [1, 2)
    exponent = math.floor(math.log2(value))
    mantissa = value / (2**exponent)

    # Bias the exponent (5 bits, bias = 15)
    exponent_bias = 15
    biased_exponent = exponent + exponent_bias

    # Handle overflow/underflow
    if biased_exponent <= 0:
        # Underflow to zero
        return 0
    elif biased_exponent >= 31:
        # Overflow to max value
        biased_exponent = 31
        mantissa_bits = 0b111111111
    else:
        # Convert mantissa to 9-bit representation
        # Remove implicit leading 1 and quantize to 9 bits
        mantissa_fraction = mantissa - 1.0
        mantissa_bits = int(mantissa_fraction * 512)
        # Clamp to 9 bits
        mantissa_bits = min(mantissa_bits, 0b111111111)

    # Pack into 14 bits: exponent(5) + mantissa(9)
    result = (biased_exponent << 9) | mantissa_bits

    return result


def encode_msg(obj):
    # temp = obj["temperatura"]
    # humid = obj["humedad"]
    # wind = obj["direccion_viento"]
    temp = 110.99
    humid = 110
    wind = "NO"

    wInt = [i for (i, x) in enumerate(DIRECTIONS) if x == wind][0]
    formatStr = "{:6}->{:7b}->{:24b}"
    print(formatStr.format(wind, wInt, wInt << 21))
    print(formatStr.format(humid, humid, humid << 14))
    print("{:6}->{:7}->{:24b}".format(temp, temp, float_to_14bit_no_sign(temp)))

    res = (wInt << 21) | (humid << 14) | float_to_14bit_no_sign(temp)
    print("res: {:36b}".format(res))
    print("=" * 20 * 2)

    return res
