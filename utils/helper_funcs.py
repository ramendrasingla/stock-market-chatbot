def generate_id(input_data):
    """
    Generates a unique integer ID for the given input data.

    Args:
        input_data (str, int, or any hashable object): Input data to generate the ID from.

    Returns:
        int: A unique integer ID.
    """
    return abs(hash(input_data))