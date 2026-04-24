"""
Usage:
cd $(git rev-parse --show-toplevel)/backend && PYTHONPATH=.. python db/scripts/generate_crud_schemas.py
"""

import subprocess
from datetime import datetime, timezone
from typing import Any, Optional, Set, Union, get_args, get_origin

from sqlmodel import SQLModel

import backend.db.data_models as data_models

# Path to the output file
OUTPUT_PATH = "db/dal/schemas.py"
OUTPUT_PATH_EXTERNALS = "db/externals/__init__.py"

# Track types used for imports
used_typenames: Set[str] = set()
EXCLUDED_MODELS = {"SchemaMigrations"}


def generate_crud_schemas(
    model_cls: type[SQLModel], name: str
) -> tuple[str, str, bool]:
    fields: dict[str, Any] = model_cls.model_fields
    create_fields: dict[str, tuple[type[Any], Any, dict[str, Any]]] = {}
    read_fields: dict[str, tuple[type[Any], Any, dict[str, Any]]] = {}
    update_fields: dict[str, tuple[Any, Any, dict[str, Any]]] = {}

    used_field = False

    for fname, f in fields.items():
        annotation: Any = f.annotation
        field_info: dict[str, Any] = {}
        if f.alias and f.alias != fname:
            field_info["alias"] = f.alias

        if fname in {"id", "created_at", "updated_at"}:
            read_fields[fname] = (annotation, ..., field_info)
        else:
            create_fields[fname] = (annotation, ..., field_info)
            update_fields[fname] = (Optional[annotation], None, field_info)
            read_fields[fname] = (annotation, ..., field_info)

    def render_field(name: str, typ: Any, default: Any, info: dict[str, Any]) -> str:
        nonlocal used_field
        typename = get_typename(typ)

        if info:
            used_field = True
            args = ", ".join(f"{k}={repr(v)}" for k, v in info.items())
            return (
                f"    {name}: {typename} = Field({args})"
                if default is ...
                else f"    {name}: {typename} = Field(default={default}, {args})"
            )
        return (
            f"    {name}: {typename}"
            if default is ...
            else f"    {name}: {typename} = {repr(default)}"
        )

    lines: list[str] = []

    lines.append(f"class {name}Create(WritableModel):")
    if create_fields:
        for k, (typ, default, info) in create_fields.items():
            lines.append(render_field(k, typ, default, info))
    else:
        lines.append("    pass")
    lines.append("")

    lines.append(f"class {name}Update(WritableModel):")
    if update_fields:
        for k, (typ, default, info) in update_fields.items():
            lines.append(render_field(k, typ, default, info))
    else:
        lines.append("    pass")
    lines.append("")

    lines_public: list[str] = []
    lines_public.append(f"class {name}PublicModel(ReadableModel):")
    if read_fields:
        for k, (typ, default, info) in read_fields.items():
            lines_public.append(render_field(k, typ, default, info))
    else:
        lines_public.append("    pass")
    lines_public.append("")

    return "\n".join(lines), "\n".join(lines_public), used_field


def get_typename(t: Any) -> str:
    origin = get_origin(t)
    args = get_args(t)

    if origin is Union and args:
        non_none_args = [a for a in args if a is not type(None)]
        if len(non_none_args) == 1:
            used_typenames.add("Optional")
            return f"Optional[{get_typename(non_none_args[0])}]"
        return " | ".join(get_typename(a) for a in args)

    if origin is list and args:
        used_typenames.add("list")
        return f"list[{get_typename(args[0])}]"

    if origin is dict and len(args) == 2:
        used_typenames.add("dict")
        return f"dict[{get_typename(args[0])}, {get_typename(args[1])}]"

    # ENUM FIX: track all used explicit type names (like UserProvidedOccasion)
    type_name = getattr(t, "__name__", str(t))
    used_typenames.add(type_name)
    return type_name


def emit_imports(field_used: bool) -> str:
    lines: list[str] = [
        "from pydantic import BaseModel, ConfigDict",
    ]
    if field_used:
        lines.append("from pydantic import Field  # noqa: F401")

    if "Optional" in used_typenames:
        lines.append("from typing import Optional")
    if "Any" in used_typenames:
        lines.append("from typing import Any")
    if "UUID" in used_typenames:
        lines.append("from uuid import UUID")
    if "datetime" in used_typenames:
        lines.append("from datetime import datetime")

    # Import enums used in type hints
    enum_types = [
        tname
        for tname in sorted(used_typenames)
        if tname
        not in {"Optional", "Any", "UUID", "datetime", "list", "dict", "str", "int"}
    ]
    if enum_types:
        lines.append(f"from backend.db.data_models import {', '.join(enum_types)}")

    lines.append(
        """\n
class ReadableModel(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class WritableModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)  # used for Create/Update"""
    )

    return "\n".join(lines) + "\n\n"


if __name__ == "__main__":
    all_cls: list[tuple[type[SQLModel], str]] = []
    for name, cls in vars(data_models).items():
        if (
            isinstance(cls, type)
            and issubclass(cls, SQLModel)
            and name not in EXCLUDED_MODELS
            and cls.__name__ != "SQLModel"
        ):
            all_cls.append((cls, name))

    used_typenames.clear()
    class_defs: list[str] = []
    class_defs_read: list[str] = []
    field_used = False

    for model_cls, name in all_cls:
        class_def, class_def_read, model_uses_field = generate_crud_schemas(
            model_cls, name
        )
        class_defs.append(class_def)
        class_defs_read.append(class_def_read)
        field_used |= model_uses_field

    header = f"""# ---------------------------------------------
# ⚠️ AUTO-GENERATED FILE — DO NOT EDIT MANUALLY
# Source: backend/db/data_models/__init__.py
# Generated by: backend/db/scripts/generate_crud_schemas.py
# Time: {datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")}
# ---------------------------------------------

"""

    content = header + emit_imports(field_used) + "\n".join(class_defs)
    with open(OUTPUT_PATH, "w") as f:
        f.write(content)
    print(f"✅ Wrote: {OUTPUT_PATH}")

    header_read = f"""# ---------------------------------------------
# ⚠️ AUTO-GENERATED FILE — DO NOT EDIT MANUALLY
# Source: backend/db/data_models/__init__.py
# Generated by: backend/db/scripts/generate_crud_schemas.py
# Time: {datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")}
# ---------------------------------------------

"""
    content_read = header_read + emit_imports(field_used) + "\n".join(class_defs_read)
    with open(OUTPUT_PATH_EXTERNALS, "w") as f:
        f.write(content_read)
    print(f"✅ Wrote: {OUTPUT_PATH_EXTERNALS}")

    # Run Ruff format
    try:
        subprocess.run(["ruff", "format", OUTPUT_PATH], check=True)
        subprocess.run(
            ["ruff", "check", "--select", "I", "--fix", OUTPUT_PATH], check=True
        )
        subprocess.run(["ruff", "format", OUTPUT_PATH_EXTERNALS], check=True)
        subprocess.run(
            ["ruff", "check", "--select", "I", "--fix", OUTPUT_PATH_EXTERNALS],
            check=True,
        )
        print("✅ Applied ruff formatting")
    except subprocess.CalledProcessError as e:
        print(f"❌ Ruff formatting failed: {e}")
    except FileNotFoundError:
        print("⚠️ Ruff not installed. Run `pip install ruff`.")
