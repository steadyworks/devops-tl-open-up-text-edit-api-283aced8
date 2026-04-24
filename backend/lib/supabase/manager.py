import os
from typing import TYPE_CHECKING, cast

from supabase import create_client

from backend.lib.utils.common import none_throws

if TYPE_CHECKING:
    from backend.stubs.supabase import AsyncSupabaseClient


class SupabaseManager:
    def __init__(self) -> None:
        self.client: AsyncSupabaseClient = cast(
            "AsyncSupabaseClient",
            create_client(
                none_throws(os.getenv("SUPABASE_URL")),
                none_throws(os.getenv("SUPABASE_SERVICE_ROLE_KEY")),
            ),
        )
