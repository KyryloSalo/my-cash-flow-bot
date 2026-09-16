from __future__ import annotations

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
import os


class Command(BaseCommand):
    help = "Create the initial Django admin from environment variables."

    def handle(self, *args, **options):
        username = (os.environ.get("ADMIN_USERNAME") or "Admin").strip()
        email = (os.environ.get("ADMIN_EMAIL") or "admin@example.com").strip()
        password = os.environ.get("ADMIN_PASSWORD")
        if not password:
            raise CommandError("ADMIN_PASSWORD is required. It is intentionally not hardcoded in the repository.")

        user_model = get_user_model()
        with transaction.atomic():
            user, created = user_model.objects.get_or_create(
                username=username,
                defaults={
                    "email": email,
                    "is_staff": True,
                    "is_superuser": True,
                },
            )
            user.email = email
            user.is_staff = True
            user.is_superuser = True
            user.set_password(password)
            user.save()

        if created:
            self.stdout.write(self.style.SUCCESS(f"Created admin user '{username}'"))
        else:
            self.stdout.write(self.style.SUCCESS(f"Updated admin user '{username}'"))
