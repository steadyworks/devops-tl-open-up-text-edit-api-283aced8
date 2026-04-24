import os
from pathlib import Path
from typing import Optional, cast

from google import genai
from google.genai import types
from google.genai.client import AsyncClient
from pydantic import BaseModel

from backend.lib.utils.common import none_throws


class PageSchema(BaseModel):
    page_photos: list[str]
    page_message: str


class PhotobookSchema(BaseModel):
    photobook_title: str
    photobook_pages: list[PageSchema]


class Gemini:
    DEFAULT_USER_INSTRUCTION = "Create a photobook to celebrate this memory!"

    def __init__(self) -> None:
        self.__client = genai.Client(
            vertexai=True,
            project=none_throws(os.getenv("GOOGLE_VERTEX_AI_PROJECT")),
            location="global",
        )
        self.model = "gemini-2.5-flash-lite-preview-06-17"

    def get_client(self) -> AsyncClient:
        return self.__client.aio

    def build_gemini_content_from_image_understanding_job(
        self,
        image_paths: list[Path],
        user_provided_occasion: Optional[str],
        user_provided_occasion_custom_details: Optional[str],
        user_provided_context: Optional[str],
    ) -> list[types.Content]:
        user_instructions = f"""The occasion was a {user_provided_occasion or user_provided_occasion_custom_details or "great memory"}. {user_provided_context or Gemini.DEFAULT_USER_INSTRUCTION}"""

        parts: list[types.Part] = []

        # Build structured prompt content with image parts
        parts.append(types.Part.from_text(text="<request>\n<photos>\n"))

        for _idx, path in enumerate(image_paths):
            with open(path, "rb") as f:
                raw_bytes = f.read()

            # FIXME: hardecoded mime
            image_part = types.Part.from_bytes(data=raw_bytes, mime_type="image/png")

            parts.append(types.Part.from_text(text=f"<photo><id>{path.name}</id><img>"))
            parts.append(image_part)
            parts.append(types.Part.from_text(text="</img></photo>\n"))

        parts.append(types.Part.from_text(text="</photos>\n<instruction>\n"))
        parts.append(types.Part.from_text(text=user_instructions))
        parts.append(types.Part.from_text(text="\n</instruction>\n</request>"))

        return [types.Content(role="user", parts=parts)]

    def build_gemini_config_from_image_understanding_job(
        self,
    ) -> types.GenerateContentConfig:
        sys_prompt = """The user will give you a structured XML like request that specifies some photos (n = 1 - 100) and their metadata, as well as some instructions, such as
<request>
  <photos>
  <photo><id>123.png</id><img>[image bytes]</img></photo>
  <photo><id>abc.png</id><img>[image bytes]</img></photo>
  <photo><id>a23.png</id><img>[image bytes]</img></photo>
  <photo><id>b45.jpg</id><img>[image bytes]</img></photo>
  </photos>
  <instruction>
    I'm creating a photo book to celebrate a memory with my girlfriend. 
  </instruction>
</request>

With the request, the user is trying to create a photobook. Use all that you can infer from the uploaded photos and do the following.
    1. Group the photos into pages. Each page can have 1-6 photos. You should group by subject, location, time, or anything you see fit. Each page should have a meaningful and coherent theme. For the photobook you create, come up with an overall short title less than 10 words.
    2. For each page, optionally write a message in 1-4 sentences to celebrate the occasion identified by the photos you chose on that page if you see fit. Remember: you don't have to hit 4 sentences. If there is a quirky one-liner that really highlights the memory, use that.
    3. Remember: The message should sound super natural as if the user is trying to convey the message to the photobook viewer. Use informal languages throughout, and intentionally make playful grammar mistakes as if a human would. Use emojis wherever you see fit that adds to the message. Don't use words that are fancy or over the top so the message sounds cringe or insincere. 
    4. Tailor your message to extract/address as many details as possible from the photos and the user instruction. When possible, call out things you observe from the photos or think relevant from the user provided context, rather than use generic words.
    5. Your generated messages and tone/style should be tailored towards the user instructions and adhere to the overall theme/mood. For example, if the user instructions mention they are with friends, the message should be more playful. If the user instructions mention with a partner, the message should be more romantic.
    6. **Almost never use words like "so", "such"** as overusing them easily sound unnatural or cringe. The generated messages should NOT feel reptitive across pages.

To recap, your job is to understand the user instructions, identify the grouping and return a JSON in the following example format:

{
    "photobook_title": "Our trip to Japan",
    "photobook_pages": [
        {"page_photos": ["123.png", "abc.png"], "page_message": "<page message for page 1>"},
        {"page_photos": ["a23.png", "b45.png"], "page_message": "<page message for page 2>"},
    ]
}
"""

        return types.GenerateContentConfig(
            temperature=1.0,
            top_p=0.95,
            max_output_tokens=65535,
            safety_settings=[
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                    threshold=types.HarmBlockThreshold.OFF,
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                    threshold=types.HarmBlockThreshold.OFF,
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                    threshold=types.HarmBlockThreshold.OFF,
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
                    threshold=types.HarmBlockThreshold.OFF,
                ),
            ],
            system_instruction=[types.Part.from_text(text=sys_prompt)],
            thinking_config=types.ThinkingConfig(thinking_budget=0),
            response_mime_type="application/json",
            response_schema=PhotobookSchema,
        )

    async def run_image_understanding_job(
        self,
        image_paths: list[Path],
        user_provided_occasion: Optional[str],
        user_provided_occasion_custom_details: Optional[str],
        user_provided_context: Optional[str],
    ) -> PhotobookSchema:
        contents = self.build_gemini_content_from_image_understanding_job(
            image_paths,
            user_provided_occasion,
            user_provided_occasion_custom_details,
            user_provided_context,
        )
        config = self.build_gemini_config_from_image_understanding_job()

        # Stream and collect output
        chunks = await self.get_client().models.generate_content_stream(
            model=self.model,
            contents=cast("types.ContentListUnion", contents),
            config=config,
        )
        response_text = ""
        async for chunk in chunks:
            if chunk.text is not None:
                response_text += chunk.text
        return PhotobookSchema.model_validate_json(response_text)
