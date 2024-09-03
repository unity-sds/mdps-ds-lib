from src.uds_lib.stage_in_out.search_collections_abstract import SearchCollectionsAbstract


class SearchCollectionsCmr(SearchCollectionsAbstract):
    def search(self, **kwargs):
        raise NotImplementedError()
