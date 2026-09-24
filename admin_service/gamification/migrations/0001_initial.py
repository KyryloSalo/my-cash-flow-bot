import django.db.models.deletion
import django.utils.timezone
import uuid
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='GamificationProfile',
            fields=[
                ('user_id', models.BigIntegerField(primary_key=True, serialize=False)),
                ('mascot', models.CharField(blank=True, choices=[('bob', 'Боб'), ('capi', 'Капі')], default='', max_length=8)),
                ('mascot_selected_at', models.DateTimeField(blank=True, null=True)),
                ('timezone_name', models.CharField(default='Europe/Kyiv', max_length=64)),
                ('pending_timezone_name', models.CharField(blank=True, default='', max_length=64)),
                ('pending_timezone_effective_at', models.DateTimeField(blank=True, null=True)),
                ('timezone_changed_at', models.DateTimeField(blank=True, null=True)),
                ('motion_enabled', models.BooleanField(default=True)),
                ('started_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('rule_version', models.CharField(default='v1', max_length=16)),
                ('revision', models.PositiveIntegerField(default=0)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'gamification_profiles',
            },
        ),
        migrations.CreateModel(
            name='ProcessedGamificationEvent',
            fields=[
                ('event_id', models.UUIDField(primary_key=True, serialize=False)),
                ('actor_user_id', models.BigIntegerField()),
                ('processed_at', models.DateTimeField(default=django.utils.timezone.now)),
            ],
            options={
                'db_table': 'gamification_processed_events',
            },
        ),
        migrations.CreateModel(
            name='ShieldAccrualState',
            fields=[
                ('user_id', models.BigIntegerField(primary_key=True, serialize=False)),
                ('anchor_lifetime_active_days', models.PositiveIntegerField(blank=True, null=True)),
                ('last_cycle', models.PositiveIntegerField(default=0)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'gamification_shield_accrual_states',
            },
        ),
        migrations.CreateModel(
            name='StreakState',
            fields=[
                ('user_id', models.BigIntegerField(primary_key=True, serialize=False)),
                ('current_streak', models.PositiveIntegerField(default=0)),
                ('best_streak', models.PositiveIntegerField(default=0)),
                ('lifetime_active_days', models.PositiveIntegerField(default=0)),
                ('shield_balance', models.PositiveSmallIntegerField(default=0)),
                ('last_active_date', models.DateField(blank=True, null=True)),
                ('had_broken_streak', models.BooleanField(default=False)),
                ('inactive_gap', models.PositiveIntegerField(default=0)),
                ('comeback_run', models.PositiveIntegerField(default=0)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'gamification_streak_states',
            },
        ),
        migrations.CreateModel(
            name='AchievementGrant',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('user_id', models.BigIntegerField()),
                ('achievement_key', models.CharField(max_length=64)),
                ('level_key', models.PositiveIntegerField(default=0)),
                ('scope_key', models.CharField(blank=True, default='', max_length=128)),
                ('trigger_event_id', models.UUIDField()),
                ('granted_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('context', models.JSONField(blank=True, default=dict)),
            ],
            options={
                'db_table': 'gamification_achievement_grants',
                'indexes': [models.Index(fields=['user_id', '-granted_at'], name='gam_grant_user_time_idx')],
                'constraints': [models.UniqueConstraint(fields=('user_id', 'achievement_key', 'level_key', 'scope_key'), name='uniq_gamification_grant_subject')],
            },
        ),
        migrations.CreateModel(
            name='AchievementProgress',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('user_id', models.BigIntegerField()),
                ('achievement_key', models.CharField(max_length=64)),
                ('current_value', models.PositiveIntegerField(default=0)),
                ('best_value', models.PositiveIntegerField(default=0)),
                ('context', models.JSONField(blank=True, default=dict)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'gamification_achievement_progress',
                'constraints': [models.UniqueConstraint(fields=('user_id', 'achievement_key'), name='uniq_gamification_progress_user_key')],
            },
        ),
        migrations.CreateModel(
            name='GamificationCommandReceipt',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('user_id', models.BigIntegerField()),
                ('command', models.CharField(max_length=48)),
                ('idempotency_key', models.CharField(max_length=128)),
                ('request_hash', models.CharField(max_length=64)),
                ('response_payload', models.JSONField(default=dict)),
                ('created_at', models.DateTimeField(default=django.utils.timezone.now)),
            ],
            options={
                'db_table': 'gamification_command_receipts',
                'indexes': [models.Index(fields=['user_id', '-created_at'], name='gam_cmd_receipt_user_idx')],
                'constraints': [models.UniqueConstraint(fields=('user_id', 'command', 'idempotency_key'), name='gam_cmd_receipt_unique')],
            },
        ),
        migrations.CreateModel(
            name='GamificationDay',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('user_id', models.BigIntegerField()),
                ('day_seq', models.PositiveIntegerField()),
                ('local_date', models.DateField()),
                ('timezone_name', models.CharField(max_length=64)),
                ('starts_at', models.DateTimeField()),
                ('ends_at', models.DateTimeField()),
                ('status', models.CharField(choices=[('pending', 'Pending'), ('active', 'Active'), ('frozen', 'Frozen'), ('missed', 'Missed')], default='pending', max_length=12)),
                ('activated_at', models.DateTimeField(blank=True, null=True)),
                ('finalized_at', models.DateTimeField(blank=True, null=True)),
                ('created_at', models.DateTimeField(default=django.utils.timezone.now)),
            ],
            options={
                'db_table': 'gamification_days',
                'indexes': [models.Index(fields=['user_id', '-local_date'], name='gam_day_user_date_idx')],
                'constraints': [models.UniqueConstraint(fields=('user_id', 'day_seq'), name='uniq_gamification_user_day_seq'), models.UniqueConstraint(fields=('user_id', 'local_date'), name='uniq_gamification_user_local_date')],
            },
        ),
        migrations.CreateModel(
            name='GamificationEventOutbox',
            fields=[
                ('event_id', models.UUIDField(primary_key=True, serialize=False)),
                ('event_type', models.CharField(max_length=64)),
                ('actor_user_id', models.BigIntegerField()),
                ('space_id', models.BigIntegerField(blank=True, null=True)),
                ('entity_type', models.CharField(max_length=32)),
                ('entity_id', models.CharField(max_length=128)),
                ('accepted_at', models.DateTimeField()),
                ('input_method', models.CharField(default='manual', max_length=24)),
                ('payload', models.JSONField(blank=True, default=dict)),
                ('status', models.CharField(choices=[('pending', 'Pending'), ('processing', 'Processing'), ('processed', 'Processed'), ('ignored', 'Ignored'), ('failed', 'Failed')], default='pending', max_length=12)),
                ('attempts', models.PositiveSmallIntegerField(default=0)),
                ('available_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('processed_at', models.DateTimeField(blank=True, null=True)),
                ('last_error_code', models.CharField(blank=True, default='', max_length=64)),
                ('created_at', models.DateTimeField(default=django.utils.timezone.now)),
            ],
            options={
                'db_table': 'gamification_event_outbox',
                'indexes': [models.Index(fields=['status', 'available_at', 'accepted_at'], name='gam_outbox_pending_idx'), models.Index(fields=['actor_user_id', 'accepted_at'], name='gam_outbox_actor_idx')],
                'constraints': [models.UniqueConstraint(fields=('event_type', 'entity_type', 'entity_id'), name='uniq_gamification_event_entity')],
            },
        ),
        migrations.CreateModel(
            name='GamificationNotification',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('user_id', models.BigIntegerField()),
                ('source_event_id', models.UUIDField(unique=True)),
                ('grant_ids', models.JSONField(default=list)),
                ('claim_token_hash', models.CharField(blank=True, default='', max_length=64)),
                ('claimed_device_hash', models.CharField(blank=True, default='', max_length=64)),
                ('claim_expires_at', models.DateTimeField(blank=True, null=True)),
                ('acknowledged_at', models.DateTimeField(blank=True, null=True)),
                ('seen_at', models.DateTimeField(blank=True, null=True)),
                ('created_at', models.DateTimeField(default=django.utils.timezone.now)),
            ],
            options={
                'db_table': 'gamification_notifications',
                'indexes': [models.Index(fields=['user_id', 'acknowledged_at', 'created_at'], name='gam_notice_user_ack_idx')],
            },
        ),
        migrations.CreateModel(
            name='GamificationTimezoneChange',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('user_id', models.BigIntegerField()),
                ('previous_timezone_name', models.CharField(max_length=64)),
                ('new_timezone_name', models.CharField(max_length=64)),
                ('requested_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('effective_at', models.DateTimeField()),
                ('applied_at', models.DateTimeField(blank=True, null=True)),
            ],
            options={
                'db_table': 'gamification_timezone_changes',
                'indexes': [models.Index(fields=['user_id', '-requested_at'], name='gam_tz_user_time_idx')],
            },
        ),
        migrations.CreateModel(
            name='PinnedAchievement',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('user_id', models.BigIntegerField()),
                ('achievement_key', models.CharField(max_length=64)),
                ('position', models.PositiveSmallIntegerField()),
                ('created_at', models.DateTimeField(default=django.utils.timezone.now)),
            ],
            options={
                'db_table': 'gamification_pinned_achievements',
                'indexes': [models.Index(fields=['user_id', 'position'], name='gam_pin_user_position_idx')],
                'constraints': [models.UniqueConstraint(fields=('user_id', 'achievement_key'), name='gam_pin_user_key_unique'), models.UniqueConstraint(fields=('user_id', 'position'), name='gam_pin_user_position_unique')],
            },
        ),
        migrations.CreateModel(
            name='ActivityContribution',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('user_id', models.BigIntegerField()),
                ('kind', models.CharField(default='transaction', max_length=24)),
                ('entity_type', models.CharField(max_length=32)),
                ('entity_id', models.CharField(max_length=128)),
                ('input_method', models.CharField(blank=True, default='', max_length=24)),
                ('accepted_at', models.DateTimeField()),
                ('created_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('day', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='contributions', to='gamification.gamificationday')),
            ],
            options={
                'db_table': 'gamification_activity_contributions',
                'indexes': [models.Index(fields=['user_id', 'input_method', 'day'], name='gam_contrib_method_day_idx')],
                'constraints': [models.UniqueConstraint(fields=('user_id', 'kind', 'entity_type', 'entity_id'), name='uniq_gamification_contribution_entity')],
            },
        ),
        migrations.CreateModel(
            name='ShieldLedger',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('user_id', models.BigIntegerField()),
                ('entitlement_key', models.CharField(max_length=64)),
                ('delta', models.SmallIntegerField()),
                ('balance_after', models.PositiveSmallIntegerField()),
                ('reason', models.CharField(max_length=32)),
                ('trigger_event_id', models.UUIDField(blank=True, null=True)),
                ('created_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('day', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='shield_entries', to='gamification.gamificationday')),
            ],
            options={
                'db_table': 'gamification_shield_ledger',
                'indexes': [models.Index(fields=['user_id', '-created_at'], name='gam_shield_user_time_idx')],
                'constraints': [models.UniqueConstraint(fields=('user_id', 'entitlement_key'), name='uniq_gamification_shield_entitlement'), models.CheckConstraint(condition=models.Q(('balance_after__lte', 2)), name='gamification_shield_balance_lte_two')],
            },
        ),
    ]
