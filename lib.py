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

SENSOR_1_KEY = "sensor 1"


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
    print("Computed: {:14b}".format(result))
    print("Exponent: {:b}".format(biased_exponent << 9))
    print("Mantissa: {:14b}".format(mantissa_bits))

    return result


def extract_mantissa_exponent(value_14bit):
    """
    Extracts mantissa and exponent from a 14-bit integer.
    Format: 5-bit exponent (MSB) + 9-bit mantissa (LSB)

    Args:
        value_14bit: 14-bit integer value

    Returns:
        tuple: (mantissa, exponent)
    """
    # Extract exponent (bits 9-13)
    exponent = (value_14bit & 0b11111000000000) >> 9

    # Extract mantissa (bits 0-8)
    mantissa = value_14bit & 0b00000111111111

    print("Received: {:14b}".format(value_14bit))
    print("Exponent: {:b}".format(exponent))
    print("Mantissa: {:14b}".format(mantissa))
    return (mantissa, exponent)


def custom_14bit_to_float(mantissa, exponent):
    """
    Converts mantissa and exponent back to decimal value.
    Format: 5-bit exponent + 9-bit mantissa (no sign)

    Args:
        mantissa: 9-bit mantissa value (0-511)
        exponent: 5-bit exponent value (0-31)

    Returns:
        float: The decimal value
    """
    exponent_bias = 15

    # Handle zero case
    if exponent == 0 and mantissa == 0:
        return 0.0

    # Reconstruct the mantissa (add implicit leading 1)
    mantissa_value = 1.0 + (mantissa / 512.0)

    # Unbias the exponent
    actual_exponent = exponent - exponent_bias

    # Calculate final value
    value = mantissa_value * (2**actual_exponent)

    return value


def decode_14bit(value_14bit):
    """
    Converts a 14-bit integer back to a normal float.
    Format: 5-bit exponent + 9-bit mantissa (no sign)

    Args:
        value_14bit: 14-bit integer value

    Returns:
        float: The decoded floating-point value
    """
    mantissa, exponent = extract_mantissa_exponent(value_14bit)
    return custom_14bit_to_float(mantissa, exponent)


def encode_msg(obj) -> str:
    temp = obj["temperatura"]
    humid = obj["humedad"]
    wind = obj["direccion_viento"]
    # temp = 110.99
    # humid = 110
    # wind = "NO"

    wInt = [i for (i, x) in enumerate(DIRECTIONS) if x == wind][0]
    formatStr = "{:6}->{:7b}->{:24b}"
    print(formatStr.format(wind, wInt, wInt << 21))
    print(formatStr.format(humid, humid, humid << 14))
    tempInt = float_to_14bit_no_sign(temp)
    print("{:6.2f}->{:7.2f}->{:24b}".format(temp, temp, tempInt))

    res = (wInt << 21) | (humid << 14) | tempInt
    print("res: {:36b}".format(res))
    print("bytes: {}".format(res.to_bytes(3)))

    return res.to_bytes(3)


def decode_msg(msg: bytes) -> dict:
    resInt = int.from_bytes(msg)
    wInt = resInt & 0b111000000000000000000000
    wind = DIRECTIONS[wInt >> 21]

    humid = (resInt & 0b00011111110000000000000) >> 14
    temp = decode_14bit(resInt & 0b000000000011111111111111)

    return NewDataRow(temp, humid, wind)


def NewDataRow(temp: float, humidity: int, wind: str) -> dict:
    return {
        "temperatura": temp,
        "humedad": humidity,
        "direccion_viento": wind,
    }
