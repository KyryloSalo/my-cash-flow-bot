from __future__ import annotations

import hashlib
import secrets
import time

from django.http import HttpRequest

from users.models import TelegramUser

ACCOUNT_DELETION_CONFIRMATION_TEXT = "ВИДАЛИТИ"
ACCOUNT_DELETION_CHALLENGE_TTL_SECONDS = 10 * 60
SESSION_CHALLENGE_HASH_KEY = "miniapp_account_deletion_challenge_hash"
SESSION_CHALLENGE_EXPIRES_AT_KEY = "miniapp_account_deletion_challenge_expires_at"
SESSION_CHALLENGE_USER_ID_KEY = "miniapp_account_deletion_challenge_user_id"


class AccountDeletionChallengeError(ValueError):
    def __init__(self, code: str, message: str, *, status: int = 400):
        self.code = code
        self.status = status
        super().__init__(message)


def issue_account_deletion_challenge(request: HttpRequest, *, user: TelegramUser) -> str:
    challenge = secrets.token_urlsafe(32)
    request.session[SESSION_CHALLENGE_HASH_KEY] = hashlib.sha256(challenge.encode("utf-8")).hexdigest()
    request.session[SESSION_CHALLENGE_EXPIRES_AT_KEY] = int(time.time()) + ACCOUNT_DELETION_CHALLENGE_TTL_SECONDS
    request.session[SESSION_CHALLENGE_USER_ID_KEY] = int(user.tg_user_id)
    return challenge


def consume_account_deletion_challenge(
    request: HttpRequest,
    *,
    user: TelegramUser,
    challenge: str,
) -> None:
    expected_hash = str(request.session.pop(SESSION_CHALLENGE_HASH_KEY, "") or "")
    expires_at = request.session.pop(SESSION_CHALLENGE_EXPIRES_AT_KEY, 0)
    challenge_user_id = request.session.pop(SESSION_CHALLENGE_USER_ID_KEY, 0)
    try:
        expires_at = int(expires_at or 0)
        challenge_user_id = int(challenge_user_id or 0)
    except (TypeError, ValueError) as exc:
        raise AccountDeletionChallengeError("challenge_invalid", "Deletion challenge is invalid.") from exc
    provided_hash = hashlib.sha256(str(challenge or "").encode("utf-8")).hexdigest()
    if (
        not expected_hash
        or not secrets.compare_digest(provided_hash, expected_hash)
        or challenge_user_id != int(user.tg_user_id)
    ):
        raise AccountDeletionChallengeError("challenge_invalid", "Deletion challenge is invalid.")
    if expires_at < int(time.time()):
        raise AccountDeletionChallengeError("challenge_expired", "Deletion challenge has expired.", status=409)
