"""Step 1 of every table: create it, with its indexes."""

from django.db import migrations

from taskiq_postgresql.contrib.django.operations import RunTaskiqStep


class Migration(migrations.Migration):
    """Create the tables ``TASKIQ_POSTGRESQL_TARGETS`` names, and their indexes."""

    initial = True
    dependencies = ()
    operations = (RunTaskiqStep(1),)
