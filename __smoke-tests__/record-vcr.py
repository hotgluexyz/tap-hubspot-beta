from hotglue_smoke_test.vcr.tap import VCRTapTestRunner


class Runner(VCRTapTestRunner):
    # Legacy OAuth forms can contain customer-specific callback URLs.
    TOKEN_KEYS = [*VCRTapTestRunner.TOKEN_KEYS, "redirect_uri"]
    FILTER_HEADERS = [
        *VCRTapTestRunner.FILTER_HEADERS,
        "cookie",
        "report-to",
        "nel",
        "cf-ray",
        "x-hubspot-correlation-id",
    ]

    PRESERVE_KEYS = {
        "id",  # Primary key and request chaining.
        "after",  # V3 pagination cursor.
        "total",  # Search time-bucket sizing.
        "expires_in",  # OAuth refresh timing.
        "archived",  # Preserve archived-record semantics.
        "archivedAt",  # Archived-product filtering and bookmark.
        "updatedAt",  # Subscription replication key.
        "hs_lastmodifieddate",  # Search filter boundary.
        "hs_object_id",  # Search ID-based pagination.
        "name",  # Property-definition schema metadata.
        "type",
        "fieldType",
        "deleted",
        "dataSensitivity",
    }

    def module(self):
        return "tap_hubspot_beta.tap"

    def launch(self):
        from tap_hubspot_beta.tap import Taphubspot

        Taphubspot.cli()


if __name__ == "__main__":
    Runner.main()
