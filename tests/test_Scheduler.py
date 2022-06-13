

# def test_schedule_missing_sample(scheduler):
#     result = scheduler.schedule_sample('blah')
#
#     assert result is None
#

def test_schedule_real_sample(scheduler):

    result = scheduler.schedule_sample('BioRec003_MX496132_negLIPIDS_postTsamouri020')
    print(result)

    assert result is not None
