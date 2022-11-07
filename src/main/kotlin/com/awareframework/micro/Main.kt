package net.wappet;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.logging.ConsoleHandler;
import org.graalvm.polyglot.Engine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main class of Wappet.
 */
public class Main {
    /**
     * The entrypoint main method of Wappet.
     */
    public static void main(final String[] args) {
        final Vertx vertx;
        try {
            vertx = Vertx.vertx();
        } catch (final RuntimeException ex) {
            logger.error("Failed to initialize Vert.x.", ex);
            System.exit(-1);
            return;
        }

        final Engine graalEngine;
        try {
            graalEngine = newGraalEngine();
        } catch (final RuntimeException ex) {
            logger.error("Failed to initialize the Graal engine.", ex);
            System.exit(-1);
            return;
        }

        final Wappet wappet = new Wappet(vertx, graalEngine);
        wappet.run();
    }

    Wappet(final Vertx vertx, final Engine graalEngine) {
        this.vertx = vertx;
        this.graalEngine = graalEngine;
    }

    void run() {
        this.vertx.exceptionHandler(new WappetExceptionHandler());
        this.vertx.registerVerticleFactory(new PuppyVerticleFactory(this.graalEngine));

        final ArrayList<Future> deploymentFutures = new ArrayList<>();
        try {
            deploymentFutures.add(this.vertx.deployVerticle("puppy:foo"));
        } catch (final RuntimeException ex) {
            logger.error("Unexpected error in deploying Puppy Verticles.", ex);
        }

        // Waiting for deployment to complete.
        // https://vertx.io/docs/vertx-core/java/#_future_coordination
        // https://vertx.io/docs/vertx-core/java/#_waiting_for_deployment_to_complete
        CompositeFuture.join(deploymentFutures).onComplete(puppyDeploymentResults -> {
            if (puppyDeploymentResults.succeeded()) {
                logger.info("Succeeded to deploy all Puppy Verticles.");

                // Deploying the Request Verticle after all Puppy Verticles are ready.
                vertx.deployVerticle("net.wappet.HttpRequestVerticle").onComplete(requestDeploymentResult -> {
                    if (requestDeploymentResult.succeeded()) {
                        logger.info("Succeeded to deploy the HTTP Verticle.");
                    } else {
                        logger.error("Failed to deploy the HTTP Verticle. Aborting.", requestDeploymentResult.cause());
                        vertx.close();
                    }
                });
            } else {
                logger.error("Failed to deploy Puppy Verticles. Aborting.", puppyDeploymentResults.cause());
                vertx.close();
            }
        });
    }

    private static Engine newGraalEngine() {
        return Engine.newBuilder("js")
            // Do not pass System.out / System.err directly. It will be closed when the Engine is closed.
            .logHandler(new ConsoleHandler())
            .build();
    }

    private static final Logger logger = LoggerFactory.getLogger(Wappet.class);

    private final Vertx vertx;
    private final Engine graalEngine;
}
