import json

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
    }

    def scrub_response_body(self, body, faker, cache):
        """Scrub JSON values while retaining property-definition schema metadata."""
        data = json.loads(body)
        scrubbed = json.loads(super().scrub_response_body(body, faker, cache))
        if isinstance(data, list):
            for original, fake in zip(data, scrubbed):
                if "name" in original and "fieldType" in original:
                    # Property definitions build schemas; record names remain scrubbed.
                    for key in (
                        "name",
                        "type",
                        "fieldType",
                        "deleted",
                        "dataSensitivity",
                        "sensitiveDataCategories",
                    ):
                        if key in original:
                            fake[key] = original[key]
        return json.dumps(scrubbed)

    def module(self):
        return "tap_hubspot_beta.tap"

    def launch(self):
        from tap_hubspot_beta.tap import Taphubspot

        Taphubspot.cli()


if __name__ == "__main__":
    Runner.main()
