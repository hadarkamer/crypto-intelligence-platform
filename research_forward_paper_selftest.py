"""Run the forward observer regressions through the repository CI convention."""
import unittest


def main():
    suite = unittest.defaultTestLoader.loadTestsFromNames([
        "test_forward_paper_engine",
        "test_forward_paper_store",
        "test_forward_paper_worker",
    ])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == "__main__":
    main()
