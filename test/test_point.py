import unittest

from point import Point


class TestLineProtocol(unittest.TestCase):

    def test_to_line_protocol(self):
        p = Point("test")

        p._tags = {
            "empty_tag": "",
            "none_tag": None,
            "backslash_tag": "C:\\",
            "string_tag": "hello"
        }

        p._fields = {
            "int_val": 1,
            "float_val": 1.1,
            "none_field": None,
            "bool_val": True,
            "string_val": 'hello',
        }
        self.assertEqual(
            p.to_line_protocol(),
            r'test,backslash_tag=C:\,string_tag=hello int_val=1i,float_val=1.1,bool_val=true,string_val="hello"'  # noqa: E501
        )


if __name__ == '__main__':
    unittest.main()
