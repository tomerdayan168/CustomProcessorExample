import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import java.util.HashMap;
import java.util.Map;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import static org.apache.phoenix.shaded.org.junit.Assert.assertEquals;

public class ExplodeJsonAttributeTest {

    @Test
    public void testExplodeJson() throws InitializationException {
        ExplodeJsonAttribute explodeJsonAttribute = new ExplodeJsonAttribute();
        TestRunner testRunner = TestRunners.newTestRunner(explodeJsonAttribute);

        String params = "{\"param1\": \"value1\", \"param2\": \"value2\"}";
        HashMap<String, String> attributes = new HashMap<String, String>() {{
            put("abc.param", params);
        }};

        String content = "";
        testRunner.enqueue(content, attributes);
        testRunner.setProperty("attributeToExplode", "abc.param");
        testRunner.run();

        FlowFile resultFlowFile = testRunner.getFlowFilesForRelationship(ExplodeJsonAttribute.REL_SUCCESS).get(0);
        Map<String, String> resultAttributes = resultFlowFile.getAttributes();

        // expected
        Map<String, String> attributesWithExploded = new HashMap<String, String>() {{
            put("abc.param", "{\"param1\": \"value1\", \"param2\": \"value2\"}");
            put("abc.param.param1", "value1");
            put("abc.param.param2", "value2");
        }};

        // assert
        for (Map.Entry<String, String> param: attributesWithExploded.entrySet()) {
            assertEquals(param.getValue(), resultAttributes.get(param.getKey()));
        }
    }
}