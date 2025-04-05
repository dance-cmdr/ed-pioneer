def distance(
    xyz_a: tuple[float, float, float], xyz_b: tuple[float, float, float]
) -> float:
    """Calculate the distance between two points in 3D space.
    Args:
        xyz_a (tuple[float, float, float]): The coordinates of the first point.
        xyz_b (tuple[float, float, float]): The coordinates of the second point.
    Returns:
        bool: The distance between the two points.
    """
    return (
        (xyz_a[0] - xyz_b[0]) ** 2
        + (xyz_a[1] - xyz_b[1]) ** 2
        + (xyz_a[2] - xyz_b[2]) ** 2
    ) ** 0.5
