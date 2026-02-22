import { createClient, SupabaseClient } from "@supabase/supabase-js";

// Initialize Supabase client
let supabaseClient: SupabaseClient | null = null;

export function getSupabaseClient(): SupabaseClient {
  if (!supabaseClient) {
    const url = process.env.SUPABASE_URL;
    const key = process.env.SUPABASE_ANON_KEY;

    if (!url || !key) {
      throw new Error(
        "SUPABASE_URL and SUPABASE_ANON_KEY environment variables are required"
      );
    }

    supabaseClient = createClient(url, key);
  }

  return supabaseClient;
}

/**
 * User types
 */
export type UserType = "public" | "municipality";

export interface UserProfile {
  id: string;
  email: string;
  user_type: UserType;
  created_at: string;
  updated_at?: string;
  profile_data?: Record<string, any>;
}

/**
 * Signup a new user
 */
export async function signupUser(
  email: string,
  password: string,
  userType: UserType
) {
  const supabase = getSupabaseClient();

  try {
    // Sign up with Supabase Auth
    const { data: authData, error: authError } = await supabase.auth.signUp({
      email,
      password,
      options: {
        data: {
          user_type: userType,
        },
      },
    });

    if (authError) {
      throw new Error(authError.message);
    }

    if (!authData.user) {
      throw new Error("User creation failed");
    }

    // Create user profile in database
    const { error: profileError } = await supabase.from("users").insert([
      {
        id: authData.user.id,
        email,
        user_type: userType,
      },
    ]);

    if (profileError) {
      throw new Error(`Profile creation failed: ${profileError.message}`);
    }

    return {
      success: true,
      message: "Signup successful. Please check your email to confirm.",
    };
  } catch (error) {
    throw new Error(
      `Signup error: ${error instanceof Error ? error.message : "Unknown error"}`
    );
  }
}

/**
 * Login a user
 */
export async function loginUser(email: string, password: string) {
  const supabase = getSupabaseClient();

  try {
    const { data, error } = await supabase.auth.signInWithPassword({
      email,
      password,
    });

    if (error) {
      throw new Error(error.message);
    }

    if (!data.session) {
      throw new Error("Login failed");
    }

    // Fetch user profile
    const { data: profileData, error: profileError } = await supabase
      .from("users")
      .select("*")
      .eq("id", data.user.id)
      .single();

    if (profileError) {
      throw new Error(`Profile fetch failed: ${profileError.message}`);
    }

    return {
      success: true,
      session: data.session,
      user: profileData as UserProfile,
    };
  } catch (error) {
    throw new Error(
      `Login error: ${error instanceof Error ? error.message : "Unknown error"}`
    );
  }
}

/**
 * Logout a user
 */
export async function logoutUser() {
  const supabase = getSupabaseClient();

  try {
    const { error } = await supabase.auth.signOut();

    if (error) {
      throw new Error(error.message);
    }

    return { success: true, message: "Logged out successfully" };
  } catch (error) {
    throw new Error(
      `Logout error: ${error instanceof Error ? error.message : "Unknown error"}`
    );
  }
}

/**
 * Get current user profile
 */
export async function getCurrentUserProfile(
  userId: string
): Promise<UserProfile | null> {
  const supabase = getSupabaseClient();

  try {
    const { data, error } = await supabase
      .from("users")
      .select("*")
      .eq("id", userId)
      .single();

    if (error) {
      console.error("Profile fetch error:", error);
      return null;
    }

    return data as UserProfile;
  } catch (error) {
    console.error("Error fetching user profile:", error);
    return null;
  }
}

/**
 * Update user profile
 */
export async function updateUserProfile(
  userId: string,
  updates: Partial<UserProfile>
) {
  const supabase = getSupabaseClient();

  try {
    const { data, error } = await supabase
      .from("users")
      .update(updates)
      .eq("id", userId)
      .select()
      .single();

    if (error) {
      throw new Error(error.message);
    }

    return {
      success: true,
      user: data as UserProfile,
    };
  } catch (error) {
    throw new Error(
      `Profile update error: ${error instanceof Error ? error.message : "Unknown error"}`
    );
  }
}

/**
 * Verify JWT token
 */
export async function verifyToken(token: string) {
  const supabase = getSupabaseClient();

  try {
    const { data, error } = await supabase.auth.getUser(token);

    if (error) {
      throw new Error(error.message);
    }

    return { valid: true, user: data.user };
  } catch (error) {
    return { valid: false, error };
  }
}
