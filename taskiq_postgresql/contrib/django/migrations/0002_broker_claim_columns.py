"""Step 2: the broker table's ``channel`` and ``not_before`` and their index."""

from django.db import migrations

from taskiq_postgresql.contrib.django.operations import RunTaskiqStep


class Migration(migrations.Migration):
    """
    Add the claim columns to the broker tables, a 0.4.x one included.

    Only the broker table has a step 2: the other tables are left alone.
    """

    dependencies = (("taskiq_postgresql", "0001_initial"),)
    operations = (RunTaskiqStep(2),)
