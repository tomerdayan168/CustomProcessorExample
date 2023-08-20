import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.*;

public class ExplodeJsonAttribute extends AbstractProcessor {
    public static final Relationship REL_FAILURE = new Relationship
            .Builder()
            .description("Failed adding attributes")
            .name("failure")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship
            .Builder()
            .description("Added attributes to flow file")
            .name("success")
            .build();


    public static final PropertyDescriptor ATTRIBUTE_TO_EXPLODE = new PropertyDescriptor
            .Builder()
            .name("attributeToExplode")
            .displayName("Attribute To Explode")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .build();

    public static final PropertyDescriptor EXPLODED_ATTRIBUTE_PREFIX = new PropertyDescriptor
            .Builder()
            .name("explodedAttributePrefix")
            .displayName("Exploded Attribute Prefix")
            .defaultValue("")
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .build();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return new ArrayList<PropertyDescriptor>() {{
            add(ATTRIBUTE_TO_EXPLODE);
            add(EXPLODED_ATTRIBUTE_PREFIX);
        }};
    }

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<Relationship>() {{
            add(REL_FAILURE);
            add(REL_SUCCESS);
        }};
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        // read flowFile
        FlowFile flowFile = session.get();

        // read properties
        String attributeToExplode = context.getProperty(ATTRIBUTE_TO_EXPLODE).getValue();
        String explodedAttributePrefix = context.getProperty(EXPLODED_ATTRIBUTE_PREFIX).getValue();

        // using objectMapper to read the json of attributes we want to 'explode' it's json to the flow file
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            HashMap<String, String> jsonToExplode = objectMapper.readValue(flowFile.getAttribute(attributeToExplode), HashMap.class);
            for (HashMap.Entry<String, String> jsonEntry: jsonToExplode.entrySet()) {
                if (Objects.equals(explodedAttributePrefix, "")) {
                    session.putAttribute(flowFile, attributeToExplode + "." + jsonEntry.getKey(), jsonEntry.getValue());
                } else {
                    session.putAttribute(flowFile, explodedAttributePrefix + "." + attributeToExplode + jsonEntry.getKey(), jsonEntry.getValue());
                }
            }
        } catch (IOException e) {
            session.transfer(flowFile, REL_FAILURE);
        }

        session.transfer(flowFile, REL_SUCCESS);
    }
}
