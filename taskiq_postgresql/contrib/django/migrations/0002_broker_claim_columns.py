"""Step 2 of the broker table: its ``channel`` and ``not_before`` and their index."""

from django.db import migrations

from taskiq_postgresql.contrib.django.operations import RunTaskiqStep


class Migration(migrations.Migration):
    """
    Add the claim columns to the broker tables, a 0.4.x one included.

    Only the broker tables are touched: step 2 of the result backend's table is
    another step, which migration 0003 runs.
    """

    dependencies = (("taskiq_postgresql", "0001_initial"),)
    operations = (RunTaskiqStep(2, kind="broker"),)
