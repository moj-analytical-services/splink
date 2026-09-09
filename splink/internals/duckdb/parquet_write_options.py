from dataclasses import dataclass


@dataclass(frozen=True, kw_only=True)
class ParquetWriteOptions:
    compression: str | None = None
    compression_level: int | None = None
    per_thread_output: bool = True
    file_size_bytes: str | int | None = None
    row_group_size: int | None = None

    def __post_init__(self):
        for name in (
            "compression",
            "compression_level",
            "file_size_bytes",
            "row_group_size",
        ):
            value = getattr(self, name)
            if value is None:
                continue
            allowed = {
                "compression": (str,),
                "compression_level": (int,),
                "file_size_bytes": (str, int),
                "row_group_size": (int,),
            }[name]
            if isinstance(value, bool) or not isinstance(value, allowed):
                raise TypeError(f"{name} has an invalid type")
            if isinstance(value, str) and not value.strip():
                raise ValueError(f"{name} must not be empty")
            if name in ("file_size_bytes", "row_group_size"):
                if isinstance(value, int) and value <= 0:
                    raise ValueError(f"{name} must be positive")
        if not isinstance(self.per_thread_output, bool):
            raise TypeError("per_thread_output must be a boolean")
