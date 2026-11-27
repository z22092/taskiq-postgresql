"""Step 2 of the result backend table: its ``progress`` column, ``result`` nullable."""

from django.db import migrations

from taskiq_postgresql.contrib.django.operations import RunTaskiqStep


class Migration(migrations.Migration):
    """
    Add ``progress`` to the result tables, and let ``result`` accept ``NULL``.

    A row holds progress before it holds a result, so a table an older release
    created needs both. Only the result backend tables are touched: step 2 of
    the broker's table is another step, which migration 0002 runs.
    """

    dependencies = (("taskiq_postgresql", "0002_broker_claim_columns"),)
    operations = (RunTaskiqStep(2, kind="result_backend"),)
