from __future__ import annotations

import asyncio
import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

import test_account_lifecycle_regression as harness  # noqa: E402

bot_main = harness.bot_main


class TextMessageTimeoutTests(unittest.IsolatedAsyncioTestCase):
    async def test_timeout_releases_update_loop_and_returns_recovery_message(self) -> None:
        message = harness.DummyMessage(text="кава 100")
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester"),
            message=message,
        )
        context = SimpleNamespace(user_data={}, application=SimpleNamespace(bot_data={}))

        started = asyncio.Event()
        cancelled = asyncio.Event()

        async def hanging_handler(_update, _context) -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        with (
            patch.object(bot_main, "TEXT_MESSAGE_TIMEOUT_SECONDS", 0.01),
            patch.object(bot_main, "_text_message_impl", new=hanging_handler),
        ):
            await bot_main.text_message(update, context)

        self.assertTrue(started.is_set())
        self.assertTrue(cancelled.is_set())
        self.assertEqual(len(message.replies), 1)
        self.assertIn("Перед повторенням перевірте", message.replies[0]["text"])

    async def test_downstream_timeout_error_is_not_misreported_as_handler_deadline(self) -> None:
        message = harness.DummyMessage(text="кава 100")
        update = SimpleNamespace(effective_user=SimpleNamespace(id=123), message=message)
        context = SimpleNamespace(user_data={}, application=SimpleNamespace(bot_data={}))

        async def downstream_timeout(_update, _context) -> None:
            raise TimeoutError("database timeout")

        with patch.object(bot_main, "_text_message_impl", new=downstream_timeout):
            with self.assertRaisesRegex(TimeoutError, "database timeout"):
                await bot_main.text_message(update, context)

        self.assertEqual(message.replies, [])

    async def test_hung_recovery_reply_is_bounded(self) -> None:
        class HangingMessage(harness.DummyMessage):
            async def reply_text(self, *args, **kwargs):
                await asyncio.Event().wait()

        message = HangingMessage(text="кава 100")
        update = SimpleNamespace(effective_user=SimpleNamespace(id=123), message=message)
        context = SimpleNamespace(user_data={}, application=SimpleNamespace(bot_data={}))

        async def hanging_handler(_update, _context) -> None:
            await asyncio.Event().wait()

        with (
            patch.object(bot_main, "TEXT_MESSAGE_TIMEOUT_SECONDS", 0.01),
            patch.object(bot_main, "TEXT_MESSAGE_RECOVERY_REPLY_TIMEOUT_SECONDS", 0.01),
            patch.object(bot_main, "_text_message_impl", new=hanging_handler),
        ):
            await asyncio.wait_for(bot_main.text_message(update, context), timeout=0.5)

    async def test_cancellation_resistant_recovery_reply_is_bounded(self) -> None:
        release_reply = asyncio.Event()
        reply_tasks: list[asyncio.Task[None]] = []

        class CancellationResistantMessage(harness.DummyMessage):
            async def reply_text(self, *args, **kwargs):
                task = asyncio.current_task()
                assert task is not None
                reply_tasks.append(task)
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    await release_reply.wait()

        message = CancellationResistantMessage(text="кава 100")
        update = SimpleNamespace(effective_user=SimpleNamespace(id=123), message=message)
        context = SimpleNamespace(user_data={}, application=SimpleNamespace(bot_data={}))

        async def hanging_handler(_update, _context) -> None:
            await asyncio.Event().wait()

        wrapper_task: asyncio.Task[None] | None = None
        try:
            with (
                patch.object(bot_main, "TEXT_MESSAGE_TIMEOUT_SECONDS", 0.01),
                patch.object(bot_main, "TEXT_MESSAGE_RECOVERY_REPLY_TIMEOUT_SECONDS", 0.01),
                patch.object(bot_main, "_text_message_impl", new=hanging_handler),
            ):
                wrapper_task = asyncio.create_task(bot_main.text_message(update, context))
                done, _ = await asyncio.wait({wrapper_task}, timeout=0.1)

            self.assertIn(wrapper_task, done)
        finally:
            release_reply.set()
            if wrapper_task is not None and not wrapper_task.done():
                wrapper_task.cancel()
            cleanup_tasks = ([wrapper_task] if wrapper_task is not None else []) + reply_tasks
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

    async def test_cancellation_resistant_handler_does_not_block_update_loop(self) -> None:
        message = harness.DummyMessage(text="кава 100")
        update = SimpleNamespace(effective_user=SimpleNamespace(id=123), message=message)
        context = SimpleNamespace(user_data={}, application=SimpleNamespace(bot_data={}))
        release_handler = asyncio.Event()
        handler_tasks: list[asyncio.Task[None]] = []

        async def cancellation_resistant_handler(_update, _context) -> None:
            task = asyncio.current_task()
            assert task is not None
            handler_tasks.append(task)
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release_handler.wait()

        wrapper_task: asyncio.Task[None] | None = None
        try:
            with (
                patch.object(bot_main, "TEXT_MESSAGE_TIMEOUT_SECONDS", 0.01),
                patch.object(bot_main, "TEXT_MESSAGE_CANCELLATION_GRACE_SECONDS", 0.01),
                patch.object(bot_main, "_text_message_impl", new=cancellation_resistant_handler),
            ):
                wrapper_task = asyncio.create_task(bot_main.text_message(update, context))
                done, _ = await asyncio.wait({wrapper_task}, timeout=0.5)

            self.assertIn(wrapper_task, done)
            self.assertEqual(len(message.replies), 1)
        finally:
            release_handler.set()
            if wrapper_task is not None and not wrapper_task.done():
                wrapper_task.cancel()
            cleanup_tasks = ([wrapper_task] if wrapper_task is not None else []) + handler_tasks
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

    async def test_cancellation_resistant_handler_blocks_overlapping_text_retry(self) -> None:
        first_message = harness.DummyMessage(text="кава 100")
        second_message = harness.DummyMessage(text="кава 100")
        user = SimpleNamespace(id=456)
        first_update = SimpleNamespace(effective_user=user, message=first_message)
        second_update = SimpleNamespace(effective_user=user, message=second_message)
        context = SimpleNamespace(user_data={}, application=SimpleNamespace(bot_data={}))
        release_handler = asyncio.Event()
        handler_calls = 0

        async def cancellation_resistant_handler(_update, _context) -> None:
            nonlocal handler_calls
            handler_calls += 1
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release_handler.wait()

        try:
            with (
                patch.object(bot_main, "TEXT_MESSAGE_TIMEOUT_SECONDS", 0.01),
                patch.object(bot_main, "TEXT_MESSAGE_CANCELLATION_GRACE_SECONDS", 0.01),
                patch.object(bot_main, "_text_message_impl", new=cancellation_resistant_handler),
            ):
                await asyncio.wait_for(bot_main.text_message(first_update, context), timeout=0.1)
                await asyncio.wait_for(bot_main.text_message(second_update, context), timeout=0.1)

            self.assertEqual(handler_calls, 1)
            self.assertEqual(len(second_message.replies), 1)
            self.assertIn("ще обробляється", second_message.replies[0]["text"])
        finally:
            release_handler.set()
            await asyncio.sleep(0)

    async def test_text_retry_is_blocked_during_cancellation_grace(self) -> None:
        first_message = harness.DummyMessage(text="кава 100")
        second_message = harness.DummyMessage(text="кава 100")
        user = SimpleNamespace(id=789)
        first_update = SimpleNamespace(effective_user=user, message=first_message)
        second_update = SimpleNamespace(effective_user=user, message=second_message)
        context = SimpleNamespace(user_data={}, application=SimpleNamespace(bot_data={}))
        cancellation_started = asyncio.Event()
        release_handler = asyncio.Event()
        handler_calls = 0

        async def cancellation_resistant_handler(_update, _context) -> None:
            nonlocal handler_calls
            handler_calls += 1
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancellation_started.set()
                await release_handler.wait()

        first_wrapper: asyncio.Task[None] | None = None
        try:
            with (
                patch.object(bot_main, "TEXT_MESSAGE_TIMEOUT_SECONDS", 0.01),
                patch.object(bot_main, "TEXT_MESSAGE_CANCELLATION_GRACE_SECONDS", 5.0),
                patch.object(bot_main, "_text_message_impl", new=cancellation_resistant_handler),
            ):
                first_wrapper = asyncio.create_task(bot_main.text_message(first_update, context))
                await asyncio.wait_for(cancellation_started.wait(), timeout=1.0)
                await asyncio.wait_for(bot_main.text_message(second_update, context), timeout=1.0)

            self.assertEqual(handler_calls, 1)
            self.assertEqual(len(second_message.replies), 1)
            self.assertIn("ще обробляється", second_message.replies[0]["text"])
        finally:
            release_handler.set()
            if first_wrapper is not None:
                await asyncio.gather(first_wrapper, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
