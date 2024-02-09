"""hubspot tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from typing import List, Type, Dict
from singer_sdk.helpers._compat import final


from tap_hubspot_beta.streams import (
    AccountStream,
    AssociationDealsCompaniesStream,
    AssociationDealsContactsStream,
    AssociationDealsLineItemsStream,
    AssociationContactsCompaniesStream,
    AssociationContactsTicketsStream,
    CallsStream,
    CompaniesStream,
    ContactEventsStream,
    ContactListData,
    ContactListsStream,
    ContactsStream,
    DealsStream,
    EmailEventsStream,
    EmailsStream,
    FormsStream,
    FormSubmissionsStream,
    LineItemsStream,
    ArchivedLineItemsStream,
    ListsStream,
    MeetingsStream,
    NotesStream,
    OwnersStream,
    ProductsStream,
    TasksStream,
    EngagementStream,
    DealsPipelinesStream,
    ContactsV3Stream,
    TicketsStream,
    DispositionsStream,
    MarketingEmailsStream,
    ContactSubscriptionStatusStream,
    PostalMailStream,
    CommunicationsStream,
    QuotesStream,
    AssociationQuotesDealsStream,
    ListMembershipV3Stream,
    ListSearchV3Stream,
    ArchivedCompaniesStream,
    ArchivedDealsStream,
    DealsAssociationParent,
    CurrenciesStream
)

STREAM_TYPES = [
    ContactsStream,
    ListsStream,
    CompaniesStream,
    DealsStream,
    OwnersStream,
    ContactListsStream,
    ContactListData,
    ProductsStream,
    LineItemsStream,
    ArchivedLineItemsStream,
    AssociationDealsContactsStream,
    AssociationDealsCompaniesStream,
    AssociationDealsLineItemsStream,
    AssociationContactsCompaniesStream,
    AssociationContactsTicketsStream,
    AccountStream,
    FormsStream,
    FormSubmissionsStream,
    ContactEventsStream,
    EmailEventsStream,
    EmailsStream,
    NotesStream,
    MeetingsStream,
    TasksStream,
    CallsStream,
    EngagementStream,
    DealsPipelinesStream,
    ContactsV3Stream,
    TicketsStream,
    DispositionsStream,
    MarketingEmailsStream,
    ContactSubscriptionStatusStream,
    PostalMailStream,
    CommunicationsStream,
    QuotesStream,
    AssociationQuotesDealsStream,
    ListMembershipV3Stream,
    ListSearchV3Stream,
    ArchivedCompaniesStream,
    ArchivedDealsStream,
    DealsAssociationParent,
    CurrenciesStream
]


class Taphubspot(Tap):
    """hubspot tap class."""

    name = "tap-hubspot"

    def __init__(
        self,
        config=None,
        catalog=None,
        state=None,
        parse_env_config=False,
        validate_config=True,
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, catalog, state, parse_env_config, validate_config)

    config_jsonschema = th.PropertiesList(
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property("redirect_uri", th.StringType, required=True),
        th.Property("refresh_token", th.StringType, required=True),
        th.Property("expires_in", th.IntegerType),
        th.Property("start_date", th.DateTimeType),
        th.Property("access_token", th.StringType),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        catalog = super().catalog_dict
        if self.config.get("catalog_metadata", False):
            streams = self.streams
            for stream in catalog["streams"]:
                stream_class = streams[stream["tap_stream_id"]]
                stream["stream_meta"] = {}
                if hasattr(stream_class, "load_fields_metadata"):
                    stream_class.load_fields_metadata()
                    for field in stream["schema"]["properties"]:
                        stream["schema"]["properties"][field]["field_meta"] = stream_class.fields_metadata.get(field, {})
        return catalog
    
    @final
    def load_streams(self) -> List[Stream]:
        """Load streams from discovery and initialize DAG.

        Return the output of `self.discover_streams()` to enumerate
        discovered streams.

        Returns:
            A list of discovered streams, ordered by name.
        """
        # Build the parent-child dependency DAG

        # Index streams by type
        streams_by_type: Dict[Type[Stream], List[Stream]] = {}
        for stream in self.discover_streams():
            stream_type = type(stream)
            if stream_type not in streams_by_type:
                streams_by_type[stream_type] = []
            
            
            streams_by_type[stream_type].append(stream)

        # Initialize child streams list for parents
        for stream_type, streams in streams_by_type.items():
            if stream_type.parent_stream_type:
                parents = streams_by_type[stream_type.parent_stream_type]
                for parent in parents:
                    for stream in streams:
                        parent.child_streams.append(stream)
                        self.logger.info(
                            f"Added '{stream.name}' as child stream to '{parent.name}'"
                        )
        
        # clean streams dict and use stream name as keys
        streams_by_type = {v[0].name:v[0] for v in streams_by_type.values()}


        # change name for v3 streams and hide streams with the same base name (personalized request HGI-5253)
        del_streams = []
        if self.config.get("use_legacy_streams", True):
            for stream in streams_by_type.values():
                if "_v3" in stream.name:
                    # if stream name has v3 in it remove 'v3' from the stream name 
                    base_name = stream.name.split("_v3")[0]
                    # excluding streams with the same base name from the catalog
                    if base_name in streams_by_type.keys():
                        # if same name stream is a parent stream keep in catalog but change name
                        if streams_by_type[base_name].child_streams:
                            streams_by_type[base_name].name = f"is_parent_stream_{base_name}"
                        else:
                            del_streams.append(base_name)
                    # change v3 name to base_name
                    stream.name = base_name
        
        if self.config.get("use_list_selection", False):
            del_streams = del_streams + ["contact_list", "contact_list_data"]
        
        # delete streams that have same name as v3 stream
        streams_by_type = {key: value for key, value in streams_by_type.items() if key not in del_streams}

        streams = streams_by_type.values()
        return sorted(
            streams,
            key=lambda x: x.name,
            reverse=False,
        )


if __name__ == "__main__":
    Taphubspot.cli()
