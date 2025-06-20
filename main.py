import os
import json
import asyncio
import base64
import websockets
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")
assert DEEPGRAM_API_KEY, "Deepgram API key missing in .env"

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("index.html") as f:
        return f.read()

@app.websocket("/ws/audio")
async def ws_audio(ws: WebSocket):
    await ws.accept()

    uri = "wss://api.deepgram.com/v1/listen"
    headers = {
        "Authorization": f"Token {DEEPGRAM_API_KEY}",
    }

    transcript_buffer = []
    last_update_time = datetime.utcnow()
    ENDPOINT_THRESHOLD = 3  # seconds of silence to consider endpoint

    async def endpoint_monitor():
        nonlocal transcript_buffer, last_update_time
        while True:
            await asyncio.sleep(1)
            if transcript_buffer and datetime.utcnow() - last_update_time > timedelta(seconds=ENDPOINT_THRESHOLD):
                final_text = " ".join(transcript_buffer).strip()
                if final_text:
                    await ws.send_text(f"[Endpoint Triggered] Sending to LLM: {final_text}")
                    transcript_buffer.clear()

    try:
        async with websockets.connect(uri, extra_headers=headers) as dg_ws:
            async def receive_from_client():
                try:
                    while True:
                        data = await ws.receive_bytes()
                        await dg_ws.send(data)
                except Exception as e:
                    print("Client stream ended:", e)

            async def receive_from_deepgram():
                nonlocal last_update_time
                try:
                    async for msg in dg_ws:
                        res = json.loads(msg)
                        if res.get("channel", {}).get("alternatives"):
                            transcript = res["channel"]["alternatives"][0]["transcript"]
                            if transcript:
                                last_update_time = datetime.utcnow()
                                transcript_buffer.append(transcript)
                                await ws.send_text(f"Transcript: {transcript}")
                except Exception as e:
                    print("Deepgram stream ended:", e)

            await asyncio.gather(
                receive_from_client(),
                receive_from_deepgram(),
                endpoint_monitor(),
            )

    except Exception as e:
        print("WebSocket error:", e)

    finally:
        try:
            await ws.close()
        except Exception as e:
            print("Client WS already closed:", e)
