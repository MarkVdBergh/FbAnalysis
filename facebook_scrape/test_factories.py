from pprint import pprint


def create_test_page(i, type_i=None, sub_type_i=None):
    page = {'id': 'page_id_%d' % i,
            'name': 'page_name_%d' % i}
    if type_i: page.update({'type': 'type_%d' % type_i})
    if sub_type_i: page.update({'sub_type': 'sub_type_%d' % sub_type_i})
    return page


# todo: make <create_test_post> factory
def create_test_post(i):
    pass


if __name__ == '__main__':
    pprint(create_test_page(1))
    pass
