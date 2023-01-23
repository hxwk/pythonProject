# __init__  , __str__  __eq__ 等
from dataclasses import dataclass


@dataclass
class ESMeta:
    inType: str
    esNodes: str
    esIndex: str
    esType: str
    selectFields: str

    def __str__(self):
        return f'ESMeta(inType={self.inType},esNodes={self.esNodes},esIndex={self.esIndex},esType={self.esType},selectFields={self.selectFields})'


if __name__ == '__main__':
    es_str = {'inType': 'Elasticsearch',
              'esNodes': '192.168.88.166:9200',
              'esIndex': 'uprofile-policy_client',
              'esType': '_doc',
              'selectFields': 'user_id,birthday'}
    es_meta = ESMeta(**es_str)
    print(es_meta)
