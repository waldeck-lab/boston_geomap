import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location('builder',
    Path(__file__).resolve().parents[1] / 'scripts/build_redlist_default.py')
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


def taxon(tid, redlist='Sårbar (VU)', rank='Species', status='Accepted'):
    return dict(taxonId=tid, redlistCategory=redlist, category={'value': rank},
        type={'value': 'Taxonomic'}, status={'value': status},
        names=[dict(name='Old name', category={'value': 'ScientificName'}, isRecommended=False),
               dict(name='Recommended name', category={'value': 'ScientificName'}, isRecommended=True)])


class RedlistSelectionTests(unittest.TestCase):
    def test_categories_and_rank_exclude_non_redlisted_and_subspecies(self):
        data = [taxon(1), taxon(2, 'Livskraftig (LC)'), taxon(3, None),
                taxon(4, rank='Subspecies'), taxon(5, status='Synonym')]
        self.assertEqual([r['taxon_id'] for r in builder.select_rows(data)], [1])

    def test_includes_all_six_redlist_categories(self):
        rows = builder.select_rows([taxon(i+1, code) for i, code in enumerate(sorted(builder.CATEGORIES))])
        self.assertEqual(len(rows), 6)
        self.assertTrue(all(r['scientific_name'] == 'Recommended name' for r in rows))
        self.assertTrue(all(r['observations_count'] == '' for r in rows))

    def test_unknown_category_fails_instead_of_silently_including_it(self):
        with self.assertRaises(ValueError):
            builder.select_rows([taxon(1, 'Unknown (ZZ)')])

    def test_duplicate_taxon_is_rejected(self):
        with self.assertRaises(ValueError):
            builder.select_rows([taxon(1), taxon(1)])


if __name__ == '__main__':
    unittest.main()
