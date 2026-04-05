/**
 * Node.js Lambda handler that throws an error.
 */
export const handler = async (event, context) => {
    throw new Error("Intentional test error from Node.js");
};
