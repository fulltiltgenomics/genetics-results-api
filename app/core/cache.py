from typing import Any, Callable, TypeVar, cast
import json
import hashlib
import os
from pathlib import Path


T = TypeVar("T")


class DiskCache:
    def __init__(
        self,
        config: dict[str, Any],
    ):
        self.cache_dir = config.get("cache_dir", "/tmp/genetics-api-cache")
        self.max_size_gb = config.get("cache_max_size_gb", 10)
        self.cache_dir = Path(self.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_size_bytes = self.max_size_gb * 1024 * 1024 * 1024
        self.config = config
        self._ensure_cache_size()

    def _get_cache_key(self, func_name: str, *args, **kwargs) -> str:
        """Generate a cache key from function name and arguments plus data file config."""
        key_parts = [func_name, self.config["gnomad"]["file"]]
        key_parts.extend([file["file"] for file in self.config["assoc_files"]])
        key_parts.extend([file["file"] for file in self.config["finemapped_files"]])
        key_parts.extend(str(arg) for arg in args)
        key_parts.extend(f"{k}:{v}" for k, v in sorted(kwargs.items()))
        key_str = "|".join(key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()

    def _get_cache_path(self, cache_key: str) -> Path:
        """Get the full path for a cache key."""
        return self.cache_dir / f"{cache_key}.json"

    def _get_cache_size(self) -> int:
        """Get total size of cache directory in bytes."""
        total_size = 0
        for path in self.cache_dir.glob("*.json"):
            total_size += path.stat().st_size
        return total_size

    def _ensure_cache_size(self) -> None:
        """Ensure cache size is under max_size_bytes by removing oldest files if needed."""
        current_size = self._get_cache_size()
        if current_size <= self.max_size_bytes:
            return

        cache_files = sorted(
            self.cache_dir.glob("*.json"), key=lambda x: x.stat().st_mtime
        )

        for cache_file in cache_files:
            if current_size <= self.max_size_bytes:
                break
            file_size = cache_file.stat().st_size
            try:
                cache_file.unlink()
                current_size -= file_size
            except OSError:
                # skip files that can't be deleted
                continue

    def get(self, func_name: str, *args, **kwargs) -> Any | None:
        """Get a value from cache if it exists."""
        cache_key = self._get_cache_key(func_name, *args, **kwargs)
        cache_path = self._get_cache_path(cache_key)

        if not cache_path.exists():
            return None

        try:
            cache_path.touch(exist_ok=True)
            with open(cache_path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None

    def set(self, func_name: str, value: Any, *args, **kwargs) -> None:
        """Set a value in the cache."""
        cache_key = self._get_cache_key(func_name, *args, **kwargs)
        cache_path = self._get_cache_path(cache_key)

        try:
            with open(cache_path, "w") as f:
                json.dump(value, f)
            self._ensure_cache_size()
        except (TypeError, IOError):
            # if we can't serialize or write, just skip caching
            pass


def create_cached_decorator(
    config: dict[str, Any],
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Create a cache decorator with the given config."""
    cache = DiskCache(config=config)

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        """Decorator to cache function results on disk."""

        async def wrapper(*args, **kwargs) -> T:
            cached_result = cache.get(func.__name__, *args, **kwargs)
            if cached_result is not None:
                return cast(T, cached_result)
            result = await func(*args, **kwargs)
            cache.set(func.__name__, result, *args, **kwargs)
            return result

        return cast(Callable[..., T], wrapper)

    return decorator
