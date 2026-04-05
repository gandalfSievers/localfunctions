/**
 * Simple Node.js Lambda handler that echoes back the event.
 */
export const handler = async (event, context) => {
    return {
        statusCode: 200,
        body: JSON.stringify({
            message: "Hello from Node.js!",
            input: event,
        }),
    };
};
