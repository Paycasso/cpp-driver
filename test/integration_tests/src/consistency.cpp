#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
# define BOOST_TEST_MODULE cassandra
#endif

#include "cql_ccm_bridge.hpp"
#include "test_utils.hpp"
#include "policy_tools.hpp"

#include "cassandra.h"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/thread.hpp>

struct CONSISTENCY_CCM_SETUP : test_utils::MultipleNodesTest {
    CONSISTENCY_CCM_SETUP() : MultipleNodesTest(3,0) {}
};

std::string get_consistency_string(CassConsistency cl)
{
    int i = static_cast<int>(cl);
    if(i < 0 || i > 10)
        return "????";
    
    static std::string consist_name_array[] = {"ANY ", "ONE ", "TWO ", "THREE ", "QUORUM ", "ALL ", "LOCAL_QUORUM", "EACH_QUORUM"};
    
    return consist_name_array[i];
}

BOOST_FIXTURE_TEST_SUITE(consistency_tests, CONSISTENCY_CCM_SETUP)

/// --run_test=consistency_tests/simple_two_nodes
BOOST_AUTO_TEST_CASE(simple_two_nodes)
{
    test_utils::CassFuturePtr session_future(cass_cluster_connect(cluster));
    cass_future_wait(session_future.get());
    CassError rc = cass_future_error_code(session_future.get());
    
    if(rc != CASS_OK) {
        BOOST_FAIL("Failed to create session.");
    }

    ccm->decommission(2);	// we need just 2 nodes for this test case
    boost::this_thread::sleep(boost::posix_time::seconds(20));	// wait for node to be down

    test_utils::CassSessionPtr session(cass_future_get_session(session_future.get()));
    
    policy_tool pt;
    pt.create_schema(session.get(), 1); // replication_factor = 1
    
    {
      CassError init_result  = pt.init_return_error(session.get(),  12, CASS_CONSISTENCY_ONE);	// Should work
      CassError query_result = pt.query_return_error(session.get(), 12, CASS_CONSISTENCY_ONE);	// Should work
      BOOST_CHECK_EQUAL(init_result,  CASS_OK);
      BOOST_CHECK_EQUAL(query_result, CASS_OK);
    }
    {
      CassError init_result  = pt.init_return_error(session.get(),  12, CASS_CONSISTENCY_ANY);  // Should work
      CassError query_result = pt.query_return_error(session.get(), 12, CASS_CONSISTENCY_ANY);  // Should fail (ANY is for writing only)
      BOOST_CHECK_EQUAL(init_result, CASS_OK);
      BOOST_CHECK(query_result != CASS_OK);
    }
    {
      CassError init_result  = pt.init_return_error(session.get(),  12, CASS_CONSISTENCY_LOCAL_QUORUM);  // Should fail (LOCAL_QUORUM is incompatible with SimpleStrategy)
      CassError query_result = pt.query_return_error(session.get(), 12, CASS_CONSISTENCY_LOCAL_QUORUM);  // Should fail (see above)
      BOOST_CHECK(init_result  != CASS_OK);
      BOOST_CHECK(query_result != CASS_OK);
    }
    {
      CassError init_result  = pt.init_return_error(session.get(),  12, CASS_CONSISTENCY_EACH_QUORUM);  // Should fail (EACH_QUORUM is incompatible with SimpleStrategy)
      CassError query_result = pt.query_return_error(session.get(), 12, CASS_CONSISTENCY_EACH_QUORUM);  // Should fail (see above)
      BOOST_CHECK(init_result  != CASS_OK);
      BOOST_CHECK(query_result != CASS_OK);
    }
    {
      CassError init_result  = pt.init_return_error(session.get(),  12, CASS_CONSISTENCY_THREE);  // Should fail (N=2, RF=1)
      CassError query_result = pt.query_return_error(session.get(), 12, CASS_CONSISTENCY_THREE);  // Should fail (N=2, RF=1)
      BOOST_CHECK(init_result  != CASS_OK);
      BOOST_CHECK(query_result != CASS_OK);
    }
}

/// --run_test=consistency_tests/one_node_down
BOOST_AUTO_TEST_CASE(one_node_down)
{
    test_utils::CassFuturePtr session_future(cass_cluster_connect(cluster));
    cass_future_wait(session_future.get());
    CassError rc = cass_future_error_code(session_future.get());
    
    if(rc != CASS_OK) {
        BOOST_FAIL("Failed to create session.");
    }
    test_utils::CassSessionPtr session(cass_future_get_session(session_future.get()));
    
    policy_tool pt;
    pt.create_schema(session.get(), 3); // replication_factor = 3
    
    {
        // Sanity check
        CassError init_result  = pt.init_return_error(session.get(),  12, CASS_CONSISTENCY_ONE);	// Should work (N=3, RF=3)
        CassError query_result = pt.query_return_error(session.get(), 12, CASS_CONSISTENCY_ONE);	// Should work (N=3, RF=3)
        BOOST_CHECK_EQUAL(init_result,  CASS_OK);
        BOOST_CHECK_EQUAL(query_result, CASS_OK);
    }
    
    ccm->decommission(2);
    boost::this_thread::sleep(boost::posix_time::seconds(20));	// wait for node to be down

    {
        CassError init_result  = pt.init_return_error(session.get(),  12, CASS_CONSISTENCY_ONE);	// Should work (N=2, RF=3)
        CassError query_result = pt.query_return_error(session.get(), 12, CASS_CONSISTENCY_ONE);	// Should work (N=2, RF=3)
        BOOST_CHECK_EQUAL(init_result,  CASS_OK);
        BOOST_CHECK_EQUAL(query_result, CASS_OK);
    }
    {
        CassError init_result  = pt.init_return_error(session.get(),  12, CASS_CONSISTENCY_TWO);	// Should work (N=2, RF=3)
        CassError query_result = pt.query_return_error(session.get(), 12, CASS_CONSISTENCY_TWO);	// Should work (N=2, RF=3)
        BOOST_CHECK_EQUAL(init_result,  CASS_OK);
        BOOST_CHECK_EQUAL(query_result, CASS_OK);
    }
    {
        CassError init_result  = pt.init_return_error(session.get(),  12, CASS_CONSISTENCY_ALL);	// Should fail (N=2, RF=3)
        CassError query_result = pt.query_return_error(session.get(), 12, CASS_CONSISTENCY_ALL);	// Should fail (N=2, RF=3)
        BOOST_CHECK(init_result  != CASS_OK);
        BOOST_CHECK(query_result != CASS_OK);
    }
    {
        CassError init_result  = pt.init_return_error(session.get(),  12, CASS_CONSISTENCY_QUORUM);	// Should work (N=2, RF=3, quorum=2)
        CassError query_result = pt.query_return_error(session.get(), 12, CASS_CONSISTENCY_QUORUM);	// Should work (N=2, RF=3, quorum=2)
        BOOST_CHECK_EQUAL(init_result,  CASS_OK);
        BOOST_CHECK_EQUAL(query_result, CASS_OK);
    }
}

/// --run_test=consistency_tests/two_nodes_down
BOOST_AUTO_TEST_CASE(two_nodes_down)
{
    test_utils::CassFuturePtr session_future(cass_cluster_connect(cluster));
    cass_future_wait(session_future.get());
    CassError rc = cass_future_error_code(session_future.get());
    
    if(rc != CASS_OK) {
        BOOST_FAIL("Failed to create session.");
    }
    test_utils::CassSessionPtr session(cass_future_get_session(session_future.get()));
    
    policy_tool pt;
    pt.create_schema(session.get(), 3); // replication_factor = 3
    
    {
        // Sanity check
        CassError init_result  = pt.init_return_error(session.get(),  12, CASS_CONSISTENCY_ONE);	// Should work (N=3, RF=3)
        CassError query_result = pt.query_return_error(session.get(), 12, CASS_CONSISTENCY_ONE);	// Should work (N=3, RF=3)
        BOOST_CHECK_EQUAL(init_result,  CASS_OK);
        BOOST_CHECK_EQUAL(query_result, CASS_OK);
    }
    
    ccm->decommission(2);
    ccm->decommission(3);
    boost::this_thread::sleep(boost::posix_time::seconds(20));	// wait for nodes to be down
    
    {
        CassError init_result  = pt.init_return_error(session.get(),  12, CASS_CONSISTENCY_ONE);	// Should work (N=1, RF=3)
        CassError query_result = pt.query_return_error(session.get(), 12, CASS_CONSISTENCY_ONE);	// Should work (N=1, RF=3)
        BOOST_CHECK_EQUAL(init_result,  CASS_OK);
        BOOST_CHECK_EQUAL(query_result, CASS_OK);
    }
    {
        CassError init_result  = pt.init_return_error(session.get(),  12, CASS_CONSISTENCY_TWO);	// Should fail (N=1, RF=3)
        CassError query_result = pt.query_return_error(session.get(), 12, CASS_CONSISTENCY_TWO);	// Should fail (N=1, RF=3)
        BOOST_CHECK(init_result  != CASS_OK);
        BOOST_CHECK(query_result != CASS_OK);
    }
    {
        CassError init_result  = pt.init_return_error(session.get(),  12, CASS_CONSISTENCY_QUORUM);	// Should fail (N=1, RF=3, quorum=2)
        CassError query_result = pt.query_return_error(session.get(), 12, CASS_CONSISTENCY_QUORUM);	// Should fail (N=1, RF=3, quorum=2)
        BOOST_CHECK(init_result  != CASS_OK);
        BOOST_CHECK(query_result != CASS_OK);
    }
}

BOOST_AUTO_TEST_SUITE_END()
