// Import Stripe library
import Stripe from 'stripe';

// Allowed origins for CORS
const allowedOrigins = ['https://www.eloward.com', 'https://eloward.com'];

// Helper function to generate CORS headers based on the request
function getCorsHeaders(request) {
  const origin = request.headers.get('Origin');
  // Allow specific origins or default to the first allowed origin if the origin matches
  const allowedOrigin = allowedOrigins.includes(origin) ? origin : allowedOrigins[0]; 

  return {
    'Access-Control-Allow-Origin': allowedOrigin,
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, Stripe-Signature',
    'Access-Control-Max-Age': '86400', // 24 hours
  };
}

// Helper function to handle errors, ensuring CORS headers are included
function handleError(error, request) {
  console.error('Error:', error);
  const corsHeaders = getCorsHeaders(request);
  
  return new Response(JSON.stringify({ error: error.message || 'An internal error occurred' }), {
    status: error.status || 400, // Use error status if available, default to 400
    headers: { 
      'Content-Type': 'application/json',
      ...corsHeaders, // Spread the CORS headers
    },
  });
}

// Define the worker object
const stripeWorker = {
  async fetch(request, env, ctx) {
    // Special case for webhook endpoint - don't clone the request body since we need the raw body for webhook signature verification
    const url = new URL(request.url);
    if (url.pathname === '/api/webhook' && request.method === 'POST') {
      try {
        // Initialize Stripe with the secret key from environment variables
        const stripe = new Stripe(env.secret_key);
        // Pass the request directly to the webhook handler without cloning it
        return await handleWebhook(request, env, stripe);
      } catch (error) {
        return handleError(error, request);
      }
    }

    // For all other requests, proceed with CORS headers
    const corsHeaders = getCorsHeaders(request);

    // Handle CORS preflight requests
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        status: 204, // No Content
        headers: corsHeaders, // Return CORS headers for preflight
      });
    }

    let response;
    try {
      // Initialize Stripe with the secret key from environment variables
      const stripe = new Stripe(env.secret_key);

      // Internal authentication helper
      const authorizeInternal = () => {
        const provided = request.headers.get('X-Internal-Auth');
        const expected = env.INTERNAL_WRITE_KEY;
        return Boolean(expected) && provided === expected;
      };

      // Route handling for non-webhook routes
      if (url.pathname === '/api/create-checkout-session') {
        response = await handleCreateCheckoutSession(request, env, corsHeaders, stripe);
      } else if (url.pathname === '/api/create-portal-session') {
        response = await handleCreatePortalSession(request, env, corsHeaders, stripe);
      } else if (url.pathname === '/subscription/status') {
        if (request.method === 'POST') {
          response = await handleSubscriptionStatus(request, env, corsHeaders);
        } else {
          response = new Response('Method not allowed', { status: 405 });
        }
      } else if (url.pathname === '/subscription/upsert') {
        if (request.method === 'POST') {
          if (!authorizeInternal()) {
            response = new Response(JSON.stringify({ error: 'Forbidden' }), { 
              status: 403, 
              headers: { 'Content-Type': 'application/json' }
            });
          } else {
            response = await handleSubscriptionUpsert(request, env, corsHeaders);
          }
        } else {
          response = new Response('Method not allowed', { status: 405 });
        }
      } else if (url.pathname === '/health') {
        // Health check endpoint with optional cleanup
        const doCleanup = url.searchParams.get('cleanup') === 'true';
        if (doCleanup) {
          await cleanupOldWebhookEvents(env);
        }
        response = new Response(JSON.stringify({ 
          status: 'ok', 
          timestamp: new Date().toISOString(),
          cleanup_performed: doCleanup 
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      } else {
        response = new Response('Not found', { status: 404 });
      }
    } catch (error) {
      // Pass the original request to handleError
      response = handleError(error, request);
    }

    // Ensure all responses (success, errors, 404s) get CORS headers
    // Clone headers from the generated response to avoid modifying the original
    const finalHeaders = new Headers(response.headers);
    
    // Add CORS headers, potentially overwriting existing ones if needed
    for (const [key, value] of Object.entries(corsHeaders)) {
        finalHeaders.set(key, value);
    }

    // Ensure Content-Type is set for JSON responses if not already present
    if (response.body && typeof response.body === 'object' && !finalHeaders.has('Content-Type')) {
        finalHeaders.set('Content-Type', 'application/json');
    }

    // Return the final response with potentially modified headers
    return new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers: finalHeaders,
    });
  }
};

// Export the worker
export default stripeWorker;

// Handler for creating a checkout session
async function handleCreateCheckoutSession(request, env, corsHeaders, stripe) {
  if (request.method !== 'POST') {
    return new Response('Method not allowed', { status: 405 });
  }

  let data;
  try {
    data = await request.json();
  } catch (err) {
    return new Response(JSON.stringify({ error: 'Invalid JSON format' }), { 
      status: 400, 
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
  
  const { channelId, channelName, returnUrl, mode, email } = data;

  if (!channelId) {
    return new Response(JSON.stringify({ error: 'Channel ID is required' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  const finalChannelName = channelName || channelId;
  const finalReturnUrl = returnUrl || 'https://www.eloward.com/dashboard?subscription=success';

  try {
    // Determine price based on mode
    const isMonthly = mode === 'monthly';
    const priceId = isMonthly ? env.MONTHLY_PRICE_ID : env.YEARLY_PRICE_ID;
    const subscriptionType = isMonthly ? 'monthly' : 'yearly';

    const sessionConfig = {
      payment_method_types: ['card'],
      mode: 'subscription',
      success_url: finalReturnUrl,
      cancel_url: 'https://www.eloward.com/dashboard',
      client_reference_id: channelId,
      line_items: [
        {
          price: priceId,
          quantity: 1,
        }
      ],
      subscription_data: {
        metadata: {
          channel_id: channelId,
          channel_name: finalChannelName,
          subscription_type: subscriptionType
        }
      },
      metadata: {
        channel_id: channelId,
        channel_name: finalChannelName,
        subscription_type: subscriptionType
      }
    };

    // Add customer email if provided
    if (email && email.trim() && email.includes('@')) {
      sessionConfig.customer_email = email.trim();
    }

    const session = await stripe.checkout.sessions.create(sessionConfig);

    return new Response(JSON.stringify({ 
      url: session.url,
      session_id: session.id 
    }), {
      status: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('Error creating checkout session:', error);
    return new Response(JSON.stringify({ 
      error: 'Failed to create checkout session',
      details: error.message 
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}


// Handler for creating a customer portal session
async function handleCreatePortalSession(request, env, corsHeaders, stripe) {
  if (request.method !== 'POST') {
    return new Response('Method not allowed', { status: 405 });
  }

  let data;
  try {
    data = await request.json();
  } catch (err) {
    return new Response(JSON.stringify({ error: 'Invalid JSON format' }), { 
        status: 400, 
        headers: { 'Content-Type': 'application/json' }
    });
  }
  
  const { channelId, returnUrl } = data;

  if (!channelId) {
    return new Response(JSON.stringify({ error: 'Channel ID is required' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' }
    });
  }

  try {
    // Get the Stripe customer ID from the subscriptions table (with retry)
    const subscribedChannel = await retryD1Operation(
      () => env.DB.prepare('SELECT stripe_customer_id FROM subscriptions WHERE twitch_id = ?').bind(channelId).first(),
      `Portal session customer lookup for channel ${channelId}`
    );

    if (!subscribedChannel || !subscribedChannel.stripe_customer_id) {
      return new Response(JSON.stringify({ error: 'No subscription found for this channel' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      });
    }
    const customerId = subscribedChannel.stripe_customer_id;
    
    console.log(`Creating portal session for customer: ${customerId}`);

    // Create a portal session
    const session = await stripe.billingPortal.sessions.create({
      customer: customerId, // Use the retrieved customer ID
      return_url: returnUrl || 'https://eloward.com/dashboard',
    });

    return new Response(JSON.stringify({ url: session.url }), {
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error) {
    console.error('Portal session error:', error);
    error.status = 500;
    throw error; // Re-throw for centralized handling
  }
}

// Handler for Stripe webhooks
async function handleWebhook(request, env, stripe) {
  // Webhooks should not have their body modified or read before signature verification
  if (request.method !== 'POST') {
    return new Response('Method not allowed', { status: 405 });
  }

  const signature = request.headers.get('stripe-signature');
  if (!signature) {
    console.error('No Stripe signature found in the webhook request headers.'); // More specific log
    return new Response('No Stripe signature found in the request headers', { 
      status: 400 
    });
  }

  const webhookSecret = env.WEBHOOK_SECRET;
  if (!webhookSecret) {
    console.error('Webhook secret is not configured in environment variables.');
    return new Response('Webhook secret not configured', { status: 500 });
  }

  // Get the raw request body as text for signature verification
  // This is critical - we must use the exact bytes that Stripe sent
  let payload;
  try {
    payload = await request.text();
  } catch (err) {
    console.error('Failed to read webhook payload:', err);
    return new Response('Failed to read webhook payload', { status: 400 });
  }

  // --- Added Detailed Logging --- 
  console.log(`Received Stripe-Signature: ${signature}`);
  console.log(`Raw payload length: ${payload.length}`);
  // Avoid logging the full secret or sensitive payload data in production logs if possible
  // Consider logging a hash of the payload if needed for comparison: `crypto.subtle.digest('SHA-256', new TextEncoder().encode(payload))`
  // --- End Added Logging ---

  let event;
  try {
    // Use constructEventAsync for async context
    event = await stripe.webhooks.constructEventAsync(
      payload,
      signature,
      webhookSecret
    );
  } catch (err) {
    // Log the specific error from Stripe library
    console.error(`Webhook signature verification failed: ${err.message}`); 
    console.error(`Error details: ${JSON.stringify(err)}`); // Log the full error object if helpful
    return new Response(`Webhook Error: ${err.message}`, { status: 400 });
  }

  // Handle different event types
  try {
    // Log successful webhook receipt
    console.log(`Webhook verified successfully! Event type: ${event.type}, Event ID: ${event.id}`); // Added Event ID
    
    // Idempotency check - prevent duplicate event processing
    const eventId = event.id;
    
    // Idempotency for all events that modify subscription state or data
    const criticalEvents = [
      'checkout.session.completed', 
      'invoice.paid', 
      'customer.subscription.deleted', 
      'customer.subscription.updated',  // Critical: handles status changes, renewals, cancellations
      'customer.subscription.created',  // Critical: handles subscription creation (fallback to checkout)
      'invoice.payment_failed'          // Critical: handles payment failures
    ];
    
    if (criticalEvents.includes(event.type)) {
      try {
        // Quick idempotency check only for critical events
        const existingEvent = await retryD1Operation(
          () => env.DB.prepare('SELECT id FROM stripe_events WHERE stripe_event_id = ?').bind(eventId).first(),
          `Idempotency check for critical event ${eventId}`,
          1 // Only 1 retry for speed
        );
        
        if (existingEvent) {
          console.log(`Critical webhook event ${eventId} already processed. Skipping.`);
          return new Response(JSON.stringify({ received: true, duplicate: true }), {
            headers: { 'Content-Type': 'application/json' }
          });
        }
        
        // Record only critical events (non-blocking)
        retryD1Operation(
          () => env.DB.prepare('INSERT INTO stripe_events (stripe_event_id, event_type, processed_at) VALUES (?, ?, CURRENT_TIMESTAMP)').bind(eventId, event.type).run(),
          `Recording critical event ${eventId}`,
          1 // Only 1 retry, don't block webhook processing
        ).catch(error => {
          console.warn(`Failed to record critical event ${eventId}, but continuing:`, error.message);
          // Don't throw - continue processing even if event recording fails
        });
        
      } catch (idempotencyError) {
        console.warn(`Idempotency check failed for ${eventId}, proceeding:`, idempotencyError.message);
        // Continue processing - better to risk duplicate than fail
      }
    } else {
      console.log(`Processing non-critical event ${eventId} (${event.type}) without idempotency check`);
    }
    
    // Create a variable to track which handler was called
    let handlerCalled = null;
    
    switch (event.type) {
      case 'customer.subscription.created':
        await handleSubscriptionCreated(event.data.object, env, stripe);
        handlerCalled = 'subscription.created';
        break;
      case 'customer.subscription.updated':
        await handleSubscriptionUpdated(event.data.object, env, stripe);
        handlerCalled = 'subscription.updated';
        break;
      case 'customer.subscription.deleted':
        await handleSubscriptionDeleted(event.data.object, env, stripe);
        handlerCalled = 'subscription.deleted';
        break;
      case 'invoice.paid':
      case 'invoice_payment.paid':
        // Important: Invoice paid is our primary activation trigger
        console.log(`PROCESSING ${event.type} webhook (Event ID: ${event.id})`);
        await handleInvoicePaid(event.data.object, env, stripe);
        handlerCalled = event.type;
        break;
      case 'invoice.payment_failed': 
        // Handle payment failures
        await handleInvoicePaymentFailed(event.data.object, env, stripe);
        handlerCalled = 'invoice.payment_failed';
        break;
      case 'checkout.session.completed':
        await handleCheckoutSessionCompleted(event.data.object, env, stripe);
        handlerCalled = 'checkout.session.completed';
        break;
      default:
        console.log(`Unhandled event type: ${event.type}`);
        handlerCalled = 'none';
    }
    
    console.log(`Webhook handler completed: ${handlerCalled} for event: ${event.id}`);
  } catch (dbError) {
      console.error('Webhook database handler error:', dbError);
      // Return 500 if DB operations fail, so Stripe retries
      return new Response('Webhook handler failed', { status: 500 });
  }

  // Acknowledge receipt of the event with a clear content type
  return new Response(JSON.stringify({ received: true }), {
    headers: { 'Content-Type': 'application/json' }
  });
}

// --- Webhook Helper Functions --- 
// NOTE: These functions now assume env.DB is configured for this worker

// Handler for checkout session completed event (potentially more robust)
async function handleCheckoutSessionCompleted(session, env, stripe) {
    const channelId = session.client_reference_id || session.metadata?.channelId;
    const channelName = session.metadata?.channelName || channelId; // Get the Twitch username, fallback to ID
    const customerId = session.customer;
    const subscriptionId = session.subscription;
    const customerEmail = session.customer_email || session.customer_details?.email;

    // Add a small delay to reduce race conditions with other webhooks (invoice.paid might come first)
    // This gives invoice.paid a chance to process before we do
    await new Promise(resolve => setTimeout(resolve, 1000)); // Increased to 1 second for better sequencing

    if (session.mode === 'subscription' && customerId && subscriptionId && channelId && channelName) {
        console.log(`Checkout completed for subscription: ${subscriptionId}, customer: ${customerId}, channel: ${channelName} (ID: ${channelId})`);
        
        let subscriptionEndDate = null;
        let subscriptionStatus = 'incomplete'; // Default status
        try {
            // Retrieve subscription details to get the renewal date and status
            const subscription = await stripe.subscriptions.retrieve(subscriptionId);
            subscriptionStatus = subscription.status;
            
            // Get the renewal date from current_period_end
            if (subscription.current_period_end && typeof subscription.current_period_end === 'number') {
                subscriptionEndDate = new Date(subscription.current_period_end * 1000).toISOString();
                console.log(`Using subscription.current_period_end (${subscription.current_period_end}) as renewal date for subscription ${subscriptionId}`);
            } else {
                // Calculate a default renewal date (1 month from now) for new subscriptions
                const defaultEndDate = new Date();
                defaultEndDate.setMonth(defaultEndDate.getMonth() + 1);
                subscriptionEndDate = defaultEndDate.toISOString();
                console.log(`Subscription ${subscriptionId} has no current_period_end. Using calculated renewal date: ${subscriptionEndDate}`);
            }

            // Ensure metadata is on the subscription object itself for future webhooks
            if ((!subscription.metadata?.channelName || !subscription.metadata?.channelId) && channelName && channelId) {
                try {
                    await stripe.subscriptions.update(subscriptionId, {
                        metadata: {
                            channelId: channelId,
                            channelName: channelName
                        }
                    });
                    console.log(`Updated subscription ${subscriptionId} with metadata: channelId=${channelId}, channelName=${channelName}`);
                } catch (metaUpdateError) {
                    console.error(`Failed to update metadata for subscription ${subscriptionId}:`, metaUpdateError);
                    // Proceed even if metadata update fails, core data is more important
                }
            }
        } catch (subRetrieveError) {
            console.error(`Failed to retrieve subscription ${subscriptionId} during checkout completed handling:`, subRetrieveError);
            // If we can't retrieve the subscription, create a default renewal date
            const defaultEndDate = new Date();
            defaultEndDate.setMonth(defaultEndDate.getMonth() + 1);
            subscriptionEndDate = defaultEndDate.toISOString();
            console.log(`Failed to retrieve subscription. Using calculated renewal date: ${subscriptionEndDate}`);
        }

        // Initially set channel_active=0 and let invoice.paid webhook activate the subscription
        // This ensures we only activate after payment is fully processed
        const isChannelActive = 0; // Always start inactive until payment confirmed via invoice.paid

        // Use the new subscription upsert endpoint to handle subscription data
        try {
            const subscriptionData = {
                twitch_id: channelId,
                channel_name: channelName, // Will be normalized in upsert
                stripe_customer_id: customerId,
                stripe_subscription_id: subscriptionId,
                subscription_end_date: subscriptionEndDate,
                plus_active: false, // Will be activated by invoice.paid
                email: customerEmail // Include customer email if available
            };

            // Call our internal subscription upsert endpoint
            const upsertResponse = await handleSubscriptionUpsert(
                new Request('https://internal/subscription/upsert', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(subscriptionData)
                }), 
                env, 
                {}
            );

            if (!upsertResponse.ok) {
                throw new Error(`Subscription upsert failed: ${upsertResponse.status}`);
            }

            console.log(`DB: Created/updated subscription record for ${channelName} (ID: ${channelId}), SubID: ${subscriptionId}. Will be activated when payment is confirmed via invoice.paid.`);
            

        } catch (dbError) {
            console.error(`Database error during checkout completed for channel ${channelName} (SubID: ${subscriptionId}):`, dbError);
            throw dbError; // Re-throw to trigger webhook retry
        }
    } else {
        console.warn('Checkout session completed, but missing critical data (mode, customerId, subscriptionId, channelId/Name). Session:', session.id);
    }
}

// Handler for subscription created event (REDUCED RELIANCE - checkout.session.completed is preferred)
async function handleSubscriptionCreated(subscription, env, stripe) {
  const customerId = subscription.customer;
  const subscriptionId = subscription.id;
  // Attempt to get identifiers from metadata, but don't fail if missing
  const channelId = subscription.metadata?.channelId;
  const channelName = subscription.metadata?.channelName;
  
  // Get customer email for completeness
  let customerEmail = null;
  try {
    const customer = await stripe.customers.retrieve(customerId);
    customerEmail = customer.email;
  } catch (error) {
    console.warn(`Failed to retrieve customer email for subscription.created ${subscriptionId}:`, error.message);
  }

  console.log(`Subscription created event received: ${subscriptionId}, customer: ${customerId}, channel (from meta): ${channelName} (ID: ${channelId})`);
  
  // We primarily rely on checkout.session.completed now. 
  // This handler might only be useful as a fallback or for logging.
  // We won't perform DB updates here unless absolutely necessary, 
  // as checkout.session.completed should handle the initial record creation/update.

  if (!customerId || !subscriptionId) {
      console.error('Subscription created event missing customer or subscription ID. Cannot process further.');
      return; 
  }

  // Optional: Could add logic here to update the DB *if* a record with this subscriptionId 
  // already exists but is missing customerId or channel info, but it adds complexity.
  // For now, we assume checkout.session.completed handles the main logic.
  console.log(`Subscription created event for ${subscriptionId} processed (no DB action taken by default).`);

  /* // Example of potential fallback DB logic (use with caution):
  try {
    let subscriptionEndDate = null;
    if (subscription.current_period_end && typeof subscription.current_period_end === 'number') {
        subscriptionEndDate = new Date(subscription.current_period_end * 1000).toISOString();
    } else {
        console.warn(`Subscription ${subscriptionId} (created event) has invalid current_period_end.`);
    }
    const isActive = ['active', 'trialing'].includes(subscription.status) ? 1 : 0;

    // Attempt to update ONLY if the subscription ID exists
    const updateQuery = `
        UPDATE \`subscribed-channels\` SET
            stripe_customer_id = COALESCE(?, stripe_customer_id),
            channel_name = COALESCE(?, channel_name),
            twitch_id = COALESCE(?, twitch_id),
            subscription_end_date = COALESCE(?, subscription_end_date),
            active = ?, 
            updated_at = CURRENT_TIMESTAMP
        WHERE stripe_subscription_id = ?;
    `;
    const result = await env.DB.prepare(updateQuery)
        .bind(customerId, channelName, channelId, subscriptionEndDate, isActive, subscriptionId)
        .run();
       
    if (result.meta.changes > 0) {
        console.log(`DB: Updated existing record for SubID ${subscriptionId} via subscription.created event.`);
    } else {
        console.log(`DB: No existing record found for SubID ${subscriptionId} during subscription.created event (expected, handled by checkout.session.completed).`);
    }
  } catch (dbError) {
      console.error(`Database error during subscription.created fallback for SubID ${subscriptionId}:`, dbError);
      // Don't throw here, as it's a fallback
  }
  */
}

// Handler for subscription updated event
async function handleSubscriptionUpdated(subscription, env, stripe) {
  const subscriptionId = subscription.id;
  const customerId = subscription.customer;
  const status = subscription.status;
  
  // Get customer email for database sync
  let customerEmail = null;
  try {
    const customer = await stripe.customers.retrieve(customerId);
    customerEmail = customer.email;
  } catch (error) {
    console.warn(`Failed to retrieve customer email for ${customerId}:`, error.message);
    // Continue without email - it's not critical for subscription updates
  }
  
  // Complete status handling for all Stripe subscription statuses
  const shouldDeactivate = ['canceled', 'past_due', 'unpaid', 'incomplete_expired', 'paused'].includes(status);
  
  // Activate for active subscriptions and trialing subscriptions (premium during trial)
  // Also activate for incomplete if it becomes active later via invoice.paid
  const shouldActivate = ['active', 'trialing'].includes(status) && !shouldDeactivate;
  
  // Special handling for incomplete subscriptions - keep current status, let invoice.paid decide
  const shouldMaintainStatus = ['incomplete'].includes(status);
  
  let subscriptionEndDate = null;
  
  // Get the renewal date from current_period_end
  if (subscription.current_period_end && typeof subscription.current_period_end === 'number') {
    subscriptionEndDate = new Date(subscription.current_period_end * 1000).toISOString();
    console.log(`Using subscription.current_period_end (${subscription.current_period_end}) as renewal date for subscription ${subscriptionId} in updated event`);
  } else {
    // Calculate a default renewal date for subscriptions missing current_period_end
    const defaultEndDate = new Date();
    defaultEndDate.setMonth(defaultEndDate.getMonth() + 1);
    subscriptionEndDate = defaultEndDate.toISOString();
    console.log(`Subscription ${subscriptionId} (updated event) has no current_period_end. Using calculated renewal date: ${subscriptionEndDate}`);
  }

  // Attempt to get channel identifiers from metadata for logging/completeness
  const channelId = subscription.metadata?.channelId;
  const channelName = subscription.metadata?.channelName;

  console.log(`Subscription updated: ${subscriptionId}, Cust: ${customerId}, Chan: ${channelName}(${channelId}), Status: ${status}, Renewal Date: ${subscriptionEndDate || 'N/A'}, Set Active: ${shouldActivate ? 1 : (shouldDeactivate ? 0 : 'unchanged')}`);

  if (!subscriptionId) {
      console.error('Subscription updated event missing subscription ID. Cannot process.');
      return;
  }
  
  // Use the new subscription upsert endpoint to handle subscription data
  try {
    let plusActiveValue;
    if (shouldActivate) {
      plusActiveValue = true; // Activate for active/trialing subscriptions
    } else if (shouldDeactivate) {
      plusActiveValue = false; // Deactivate for failed/canceled subscriptions
    } else {
      plusActiveValue = undefined; // Maintain current status (for incomplete, etc.)
    }

    const subscriptionData = {
      twitch_id: channelId,
      channel_name: channelName, // Will be normalized in upsert
      stripe_customer_id: customerId,
      stripe_subscription_id: subscriptionId,
      subscription_end_date: subscriptionEndDate,
      plus_active: plusActiveValue,
      email: customerEmail
    };

    // Call our internal subscription upsert endpoint
    const upsertResponse = await handleSubscriptionUpsert(
      new Request('https://internal/subscription/upsert', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(subscriptionData)
      }), 
      env, 
      {}
    );

    if (!upsertResponse.ok) {
      throw new Error(`Subscription upsert failed: ${upsertResponse.status}`);
    }

    const action = shouldDeactivate ? 'deactivated' : (shouldActivate ? 'activated' : 'updated');
    console.log(`DB: Subscription ${subscriptionId} ${action} via subscription.updated (status: ${status})`);
    
  } catch (dbError) {
    console.error(`Database error during subscription.updated for SubID ${subscriptionId}:`, dbError);
    throw dbError; // Re-throw to trigger webhook retry
  }
}

// Handler for subscription deleted event
async function handleSubscriptionDeleted(subscription, env, stripe) {
  const subscriptionId = subscription.id;
  const customerId = subscription.customer;
  // Metadata might be missing here, don't rely on it
  const channelId = subscription.metadata?.channelId;
  const channelName = subscription.metadata?.channelName;
  
  console.log(`Subscription deleted event: ${subscriptionId}, Customer: ${customerId}, Channel (from meta, might be missing): ${channelName} (${channelId})`);

  if (!subscriptionId) {
    console.error('Subscription deleted event missing subscription ID. Cannot process.');
    return; // Cannot proceed without subscriptionId
  }

  // Deactivate subscription using the internal API
  try {
    const subscriptionData = {
      twitch_id: channelId || null, // May be missing in deletion events
      channel_name: channelName || null, // May be missing in deletion events
      stripe_customer_id: customerId || null,
      stripe_subscription_id: subscriptionId,
      plus_active: false // Deactivate the subscription
    };

    // Call our internal subscription upsert endpoint
    const upsertResponse = await handleSubscriptionUpsert(
      new Request('https://internal/subscription/upsert', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(subscriptionData)
      }), 
      env, 
      {}
    );

    if (!upsertResponse.ok) {
      console.warn(`Failed to deactivate subscription ${subscriptionId} via API: ${upsertResponse.status}`);
      // Try direct DB update as fallback (with retry) - also get channel_name for lol_ranks sync
      const subscriptionLookup = await retryD1Operation(
        () => env.DB.prepare(`SELECT channel_name FROM subscriptions WHERE stripe_subscription_id = ?`).bind(subscriptionId).first(),
        `Subscription lookup for fallback deactivation ${subscriptionId}`
      ).catch(() => null);
      
      const directResult = await retryD1Operation(
        () => env.DB.prepare(`UPDATE subscriptions SET plus_active = 0, updated_at = CURRENT_TIMESTAMP WHERE stripe_subscription_id = ?`).bind(subscriptionId).run(),
        `Direct subscription deactivation fallback for ${subscriptionId}`
      );
      
      // Sync to lol_ranks if we found the subscription
      if (directResult.changes > 0 && subscriptionLookup?.channel_name) {
        try {
          await syncSubscriptionStatusToRanks(env, subscriptionLookup.channel_name, false);
        } catch (syncError) {
          console.warn('Failed to sync subscription deactivation to ranks table:', syncError);
        }
      }
      
      if (directResult.changes === 0) {
        console.warn(`DB: Subscription ${subscriptionId} not found in DB for subscription.deleted event.`);
      } else {
        console.log(`DB: Marked subscription ${subscriptionId} inactive via direct query from subscription.deleted`);
      }
    } else {
      console.log(`DB: Marked subscription ${subscriptionId} inactive from subscription.deleted`);
    }
  } catch (dbError) {
    console.error(`Database error during subscription.deleted for SubID ${subscriptionId}:`, dbError);
    throw dbError; // Re-throw to trigger webhook retry
  }
}

// Handler for invoice paid event (updates end date and activates subscription)
async function handleInvoicePaid(invoice, env, stripe) {
  const subscriptionId = invoice.subscription;
  const customerId = invoice.customer;

  if (!subscriptionId || !customerId) {
      console.log(`Invoice paid event ${invoice.id} is missing subscription ID (${subscriptionId}) or customer ID (${customerId}). Ignoring.`);
      return; // Ignore invoices not linked to a subscription/customer we can identify
  }
  
  // Initialize subscription end date (renewal date)
  let subscriptionEndDate = null;
  let customerEmail = null;
  
  // Retrieve the subscription and customer to get renewal date, status, metadata, and email
  try {
    const [subscription, customer] = await Promise.all([
      stripe.subscriptions.retrieve(subscriptionId),
      stripe.customers.retrieve(customerId)
    ]);
    const status = subscription.status;
    customerEmail = customer.email;
    
    // Always set channel_active=1 when invoice.paid is received
    // This is the definitive signal that payment has been processed successfully
    const isChannelActive = 1; // Invoice paid = active subscription
    
    // Get the actual renewal date from current_period_end
    if (subscription.current_period_end && typeof subscription.current_period_end === 'number') {
        subscriptionEndDate = new Date(subscription.current_period_end * 1000).toISOString();
        console.log(`Using subscription.current_period_end (${subscription.current_period_end}) as renewal date for subscription ${subscriptionId}`);
    } else {
        // Calculate a default renewal date (1 month from now) for subscriptions
        const defaultEndDate = new Date();
        defaultEndDate.setMonth(defaultEndDate.getMonth() + 1);
        subscriptionEndDate = defaultEndDate.toISOString();
        console.log(`Subscription ${subscriptionId} (invoice.paid event) has no current_period_end. Using calculated renewal date: ${subscriptionEndDate}`);
    }

    // Get identifiers from metadata (hopefully populated by checkout.session.completed)
    const channelId = subscription.metadata?.channelId;
    const channelName = subscription.metadata?.channelName;

    console.log(`ACTIVATION: Invoice paid for subscription: ${subscriptionId}, Cust: ${customerId} (${customerEmail || 'no email'}), Chan: ${channelName}(${channelId}), Status: ${status}, Renewal Date: ${subscriptionEndDate || 'N/A'}`);

    // --- Database Update using new subscription system --- 
    try {
        const subscriptionData = {
            twitch_id: channelId,
            channel_name: channelName, // Will be normalized in upsert
            stripe_customer_id: customerId,
            stripe_subscription_id: subscriptionId,
            subscription_end_date: subscriptionEndDate,
            plus_active: true, // ACTIVATE the subscription - invoice paid = confirmed payment
            email: customerEmail // Include customer email
        };

        // Call our internal subscription upsert endpoint to activate the subscription
        const upsertResponse = await handleSubscriptionUpsert(
            new Request('https://internal/subscription/upsert', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(subscriptionData)
            }), 
            env, 
            {}
        );

        if (!upsertResponse.ok) {
            throw new Error(`Subscription activation failed: ${upsertResponse.status}`);
        }

        console.log(`DB: ACTIVATED subscription ${subscriptionId} (plus_active=true) from invoice.paid`);
        
        // Verify activation in subscriptions table (with retry)
        const verifyResult = await retryD1Operation(
            () => env.DB.prepare('SELECT plus_active FROM subscriptions WHERE stripe_subscription_id = ?').bind(subscriptionId).first(),
            `Subscription activation verification for ${subscriptionId}`
        );
            
        if (verifyResult) {
            console.log(`VERIFICATION: Subscription ${subscriptionId} plus_active status is now: ${verifyResult.plus_active}`);
            if (verifyResult.plus_active !== 1) {
                console.error(`CRITICAL: Subscription ${subscriptionId} should be plus_active=1 but is ${verifyResult.plus_active}!`);
            }
        }
    } catch(dbError) {
        console.error(`Database error during invoice.paid for SubID ${subscriptionId}:`, dbError);
        throw dbError; // Re-throw to trigger retry
    }
  } catch (error) {
      console.error(`Error retrieving subscription ${subscriptionId} during invoice.paid handling:`, error);
      
      // Even if subscription retrieval fails, we can still calculate a default renewal date
      // and activate the subscription since payment was confirmed
      const defaultEndDate = new Date();
      defaultEndDate.setMonth(defaultEndDate.getMonth() + 1);
      subscriptionEndDate = defaultEndDate.toISOString();
      console.log(`Failed to retrieve subscription. Using calculated renewal date: ${subscriptionEndDate} for database update`);
      
      try {
          // Try activating using subscription upsert with fallback data
          const fallbackSubscriptionData = {
              stripe_subscription_id: subscriptionId,
              subscription_end_date: subscriptionEndDate,
              plus_active: true // Activate since payment was confirmed
              // Note: missing twitch_id and channel_name - this fallback may not work with new schema
          };

          const fallbackResponse = await handleSubscriptionUpsert(
              new Request('https://internal/subscription/upsert', {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify(fallbackSubscriptionData)
              }), 
              env, 
              {}
          );

          if (fallbackResponse.ok) {
              console.log(`DB: ACTIVATED subscription ${subscriptionId} (plus_active=true) using calculated renewal date via fallback`);
              return; // Success, exit function
          } else {
              console.warn(`Fallback subscription activation failed for ${subscriptionId}: ${fallbackResponse.status}`);
          }
      } catch (fallbackDbError) {
          console.error(`Fallback subscription activation failed for subscription ${subscriptionId}:`, fallbackDbError);
      }
      
      if (error.type === 'StripeInvalidRequestError') {
          console.warn(`Subscription ${subscriptionId} not found via Stripe API during invoice.paid. It might have been deleted.`);
          return; // Subscription likely deleted, ignore
      }
      throw error; // Re-throw other errors
  }
}

// Handler for invoice payment failed event (marks subscription as inactive)
async function handleInvoicePaymentFailed(invoice, env, stripe) {
  const subscriptionId = invoice.subscription;
  const customerId = invoice.customer;
  
  if (!subscriptionId || !customerId) {
    console.log(`Invoice payment failed event ${invoice.id} missing subscription ID (${subscriptionId}) or customer ID (${customerId}). Ignoring.`);
    return; 
  }
  
  try {
    // Retrieve the subscription to get the current details and metadata
    const subscription = await stripe.subscriptions.retrieve(subscriptionId);
    const status = subscription.status;
    const channelId = subscription.metadata?.channelId;
    const channelName = subscription.metadata?.channelName;
    
    console.log(`Invoice payment failed for subscription: ${subscriptionId}, Cust: ${customerId}, Chan: ${channelName}(${channelId}), Status: ${status}`);
    
    // If status indicates payment issue, mark as inactive in our database
    if (['past_due', 'unpaid', 'incomplete_expired', 'canceled'].includes(status)) { // Added 'canceled' for robustness
      try {
        const subscriptionData = {
          twitch_id: channelId,
          stripe_customer_id: customerId,
          stripe_subscription_id: subscriptionId,
          plus_active: false // Deactivate due to payment failure
        };

        // Call our internal subscription upsert endpoint to deactivate
        const upsertResponse = await handleSubscriptionUpsert(
          new Request('https://internal/subscription/upsert', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(subscriptionData)
          }), 
          env, 
          {}
        );

        if (upsertResponse.ok) {
          console.log(`DB: Marked subscription ${subscriptionId} inactive due to payment failure/status (${status})`);
        } else {
          console.warn(`Failed to deactivate subscription ${subscriptionId} via API: ${upsertResponse.status}`);
          // Try direct DB update as fallback (with retry) - also get channel_name for lol_ranks sync
          const subscriptionLookup = await retryD1Operation(
            () => env.DB.prepare(`SELECT channel_name FROM subscriptions WHERE stripe_subscription_id = ?`).bind(subscriptionId).first(),
            `Subscription lookup for payment failure fallback ${subscriptionId}`
          ).catch(() => null);
          
          const directResult = await retryD1Operation(
            () => env.DB.prepare(`UPDATE subscriptions SET plus_active = 0, updated_at = CURRENT_TIMESTAMP WHERE stripe_subscription_id = ?`).bind(subscriptionId).run(),
            `Direct subscription deactivation for payment failure ${subscriptionId}`
          );
          
          // Sync to lol_ranks if we found the subscription
          if (directResult.changes > 0 && subscriptionLookup?.channel_name) {
            try {
              await syncSubscriptionStatusToRanks(env, subscriptionLookup.channel_name, false);
            } catch (syncError) {
              console.warn('Failed to sync subscription deactivation to ranks table:', syncError);
            }
          }
          
          if (directResult.changes === 0) {
            console.warn(`DB: Subscription ${subscriptionId} not found in DB for invoice.payment_failed update.`);
          } else {
            console.log(`DB: Marked subscription ${subscriptionId} inactive via direct query due to payment failure/status (${status})`);
          }
        }
      } catch (dbError) {
        console.error(`Database error during invoice.payment_failed for SubID ${subscriptionId}:`, dbError);
        throw dbError;
      }
    } else {
      console.log(`Subscription ${subscriptionId} status is ${status}. No database action taken for invoice.payment_failed.`);
    }
  } catch (error) {
    console.error(`Error retrieving subscription ${subscriptionId} during invoice.payment_failed handling:`, error);
      if (error.type === 'StripeInvalidRequestError') {
          console.warn(`Subscription ${subscriptionId} not found via Stripe API during invoice.payment_failed. It might have been deleted.`);
          return; // Subscription likely deleted, ignore
      }
      throw error; // Re-throw other errors
  }
}

// --- Subscription Management Functions ---

/**
 * Helper function to parse JSON with error handling
 */
async function parseRequestBody(request) {
  try {
    return await request.json();
  } catch (e) {
    throw new Error('Invalid JSON');
  }
}

/**
 * Helper function to create error responses
 */
function createErrorResponse(status, error, message = null, headers = {}) {
  const response = { error };
  if (message) response.message = message;
  
  return new Response(JSON.stringify(response), {
    status,
    headers: { 'Content-Type': 'application/json', ...headers }
  });
}

/**
 * Handle subscription status lookup (public endpoint)
 */
async function handleSubscriptionStatus(request, env, corsHeaders) {
  try {
    const { twitch_id } = await parseRequestBody(request);
    if (!twitch_id) {
      return createErrorResponse(400, 'Missing twitch_id parameter', null, corsHeaders);
    }

    const result = await retryD1Operation(
      () => env.DB.prepare(`SELECT plus_active, subscription_end_date FROM subscriptions WHERE twitch_id = ?`).bind(twitch_id).first(),
      `Subscription status lookup for twitch_id ${twitch_id}`
    );

    // Check if subscription is active and not expired
    const isActive = result?.plus_active && 
      (!result.subscription_end_date || new Date(result.subscription_end_date) > new Date());

    return new Response(JSON.stringify({
      plus_active: !!isActive,
      subscription_end_date: result?.subscription_end_date || null
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  } catch (error) {
    console.error(`Error fetching subscription status for twitch_id:`, error);
    return createErrorResponse(500, 'Internal Server Error', error.message, corsHeaders);
  }
}

/**
 * Webhook-optimized retry mechanism for D1 operations
 * Fast retries to stay within Stripe's 30-second webhook timeout
 */
async function retryD1Operation(operation, context = 'DB operation', maxRetries = 2, baseDelayMs = 100) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      const errorMessage = error.message || '';
      const isRetryableError = 
        errorMessage.includes('D1 DB is overloaded') ||
        errorMessage.includes('Too many requests queued') ||
        errorMessage.includes('timeout') ||
        errorMessage.includes('connection') ||
        errorMessage.includes('SQLITE_BUSY');
      
      if (isRetryableError && attempt < maxRetries) {
        // Fast exponential backoff optimized for webhooks
        const exponentialDelay = baseDelayMs * Math.pow(1.5, attempt - 1); // Gentler exponential growth
        const jitter = Math.random() * 0.2 * exponentialDelay; // 20% jitter
        const totalDelay = Math.min(exponentialDelay + jitter, 1000); // Cap at 1s for webhooks
        
        console.warn(`${context} failed (attempt ${attempt}/${maxRetries}): ${errorMessage}. Retrying in ${Math.round(totalDelay)}ms`);
        await new Promise(resolve => setTimeout(resolve, totalDelay));
      } else {
        // Log final failure
        if (isRetryableError) {
          console.error(`${context} failed after ${maxRetries} attempts: ${errorMessage}`);
        }
        throw error;
      }
    }
  }
}

/**
 * Critical operation wrapper - only for the most essential operations
 * Still webhook-optimized but with one extra retry
 */
async function retryCriticalD1Operation(operation, context = 'Critical DB operation') {
  return retryD1Operation(operation, context, 3, 100); // 3 retries max, very fast
}

/**
 * Handle subscription upsert (internal auth required)
 */
async function handleSubscriptionUpsert(request, env, corsHeaders) {
  try {
    const { twitch_id, channel_name, stripe_customer_id, stripe_subscription_id, subscription_end_date, plus_active, email } = await parseRequestBody(request);
    
    if (!twitch_id || !channel_name) {
      return createErrorResponse(400, 'Missing twitch_id or channel_name parameter', null, corsHeaders);
    }

    // Normalize channel name to lowercase for consistency
    const normalizedChannelName = channel_name.toLowerCase();

    // Upsert subscription record with critical retry logic
    const result = await retryCriticalD1Operation(
      () => env.DB.prepare(`
        INSERT INTO subscriptions (twitch_id, channel_name, email, stripe_customer_id, stripe_subscription_id, subscription_end_date, plus_active, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (twitch_id) DO UPDATE SET
          channel_name = excluded.channel_name,
          email = COALESCE(excluded.email, email),
          stripe_customer_id = COALESCE(excluded.stripe_customer_id, stripe_customer_id),
          stripe_subscription_id = COALESCE(excluded.stripe_subscription_id, stripe_subscription_id),
          subscription_end_date = COALESCE(excluded.subscription_end_date, subscription_end_date),
          plus_active = COALESCE(excluded.plus_active, plus_active),
          updated_at = CURRENT_TIMESTAMP
      `).bind(
        twitch_id,
        normalizedChannelName,
        email || null,
        stripe_customer_id || null,
        stripe_subscription_id || null,
        subscription_end_date || null,
        plus_active !== undefined ? (plus_active ? 1 : 0) : null
      ).run(),
      `Subscription upsert for ${normalizedChannelName} (${twitch_id})`
    );

    // After successful subscription update, sync the plus_active flag to lol_ranks table
    if (result.changes && result.changes > 0) {
      try {
        await syncSubscriptionStatusToRanks(env, normalizedChannelName, !!plus_active);
      } catch (syncError) {
        console.warn('Failed to sync subscription status to ranks table:', syncError);
        // Don't fail the main operation if sync fails
      }
    }

    return new Response(JSON.stringify({ 
      success: true, 
      changes: result.changes || 0,
      twitch_id,
      plus_active: !!plus_active
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  } catch (error) {
    console.error(`Error upserting subscription data:`, error);
    return createErrorResponse(500, 'Internal Server Error', error.message, corsHeaders);
  }
}

/**
 * Sync subscription status to lol_ranks table for consistent badge display
 * Optimized: uses channel_name directly, no users table lookup required
 */
async function syncSubscriptionStatusToRanks(env, channel_name, plus_active) {
  try {
    // Update plus_active in lol_ranks table and reset options if deactivating (with retry)
    let syncQuery, syncParams;
    if (plus_active) {
      // Just update plus_active when activating
      syncQuery = 'UPDATE lol_ranks SET plus_active = ? WHERE twitch_username = ?';
      syncParams = [1, channel_name];
    } else {
      // When deactivating, also reset all plus options to false
      syncQuery = 'UPDATE lol_ranks SET plus_active = ?, show_peak = ?, animate_badge = ? WHERE twitch_username = ?';
      syncParams = [0, 0, 0, channel_name];
    }
    
    const syncResult = await retryD1Operation(
      () => env.DB.prepare(syncQuery).bind(...syncParams).run(),
      `Ranks table sync for ${channel_name}`
    );
    
    if (syncResult.changes && syncResult.changes > 0) {
      const resetMsg = plus_active ? '' : ' and reset options';
      console.log(`Synced subscription status to ranks table: ${channel_name} -> plus_active: ${plus_active}${resetMsg}`);
    } else {
      console.log(`No LoL account found for ${channel_name} - subscription sync skipped`);
    }
  } catch (error) {
    console.error('Error syncing subscription status to ranks:', error);
    throw error;
  }
}

/**
 * Clean up old Stripe events to prevent database bloat
 * Should be called periodically (e.g., via scheduled worker)
 */
async function cleanupOldWebhookEvents(env, daysToKeep = 30) {
  try {
    const cutoffTimestamp = Math.floor(Date.now() / 1000) - (daysToKeep * 24 * 60 * 60);
    
    const result = await retryD1Operation(
      () => env.DB.prepare('DELETE FROM stripe_events WHERE processed_at < ?').bind(cutoffTimestamp).run(),
      'Webhook events cleanup'
    );
    
    if (result.changes && result.changes > 0) {
      console.log(`Cleaned up ${result.changes} old Stripe events (older than ${daysToKeep} days)`);
    }
  } catch (error) {
    console.error('Error cleaning up old Stripe events:', error);
  }
}
