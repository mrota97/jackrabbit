export { };
declare global {
    namespace NodeJS {
        interface ProcessEnv {
            RABBIT_URL: string;
        }
    }
}