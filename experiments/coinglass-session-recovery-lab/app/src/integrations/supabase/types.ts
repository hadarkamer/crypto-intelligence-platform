export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[]

export type Database = {
  // Allows to automatically instantiate createClient with right options
  // instead of createClient<Database, { PostgrestVersion: 'XX' }>(URL, KEY)
  __InternalSupabase: {
    PostgrestVersion: "14.15"
  }
  public: {
    Tables: {
      app_settings: {
        Row: {
          active_template_version_id: string | null
          app_name: string
          created_at: string
          id: string
          updated_at: string
        }
        Insert: {
          active_template_version_id?: string | null
          app_name?: string
          created_at?: string
          id?: string
          updated_at?: string
        }
        Update: {
          active_template_version_id?: string | null
          app_name?: string
          created_at?: string
          id?: string
          updated_at?: string
        }
        Relationships: [
          {
            foreignKeyName: "app_settings_active_template_version_id_fkey"
            columns: ["active_template_version_id"]
            isOneToOne: false
            referencedRelation: "template_versions"
            referencedColumns: ["id"]
          },
        ]
      }
      economic_event_reminders: {
        Row: {
          created_at: string
          due_at: string
          event_id: string
          id: string
          offset_minutes: number
          updated_at: string
          user_id: string | null
        }
        Insert: {
          created_at?: string
          due_at: string
          event_id: string
          id?: string
          offset_minutes: number
          updated_at?: string
          user_id?: string | null
        }
        Update: {
          created_at?: string
          due_at?: string
          event_id?: string
          id?: string
          offset_minutes?: number
          updated_at?: string
          user_id?: string | null
        }
        Relationships: [
          {
            foreignKeyName: "economic_event_reminders_event_id_fkey"
            columns: ["event_id"]
            isOneToOne: false
            referencedRelation: "economic_events"
            referencedColumns: ["id"]
          },
        ]
      }
      economic_events: {
        Row: {
          created_at: string
          created_by: string
          description: string
          event_at: string
          event_type_key: string
          id: string
          importance: number
          name: string
          occurrence_key: string | null
          recurring_schedule_id: string | null
          source_url: string | null
          updated_at: string
          updated_by: string
          workspace_id: string | null
        }
        Insert: {
          created_at?: string
          created_by?: string
          description?: string
          event_at: string
          event_type_key: string
          id?: string
          importance?: number
          name: string
          occurrence_key?: string | null
          recurring_schedule_id?: string | null
          source_url?: string | null
          updated_at?: string
          updated_by?: string
          workspace_id?: string | null
        }
        Update: {
          created_at?: string
          created_by?: string
          description?: string
          event_at?: string
          event_type_key?: string
          id?: string
          importance?: number
          name?: string
          occurrence_key?: string | null
          recurring_schedule_id?: string | null
          source_url?: string | null
          updated_at?: string
          updated_by?: string
          workspace_id?: string | null
        }
        Relationships: [
          {
            foreignKeyName: "economic_events_recurring_schedule_id_fkey"
            columns: ["recurring_schedule_id"]
            isOneToOne: false
            referencedRelation: "v2_economic_schedules"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "economic_events_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      economic_recurring_acknowledgements: {
        Row: {
          acknowledged_at: string
          occurrence_start: string
          offset_minutes: number
          schedule_id: string
          user_id: string
        }
        Insert: {
          acknowledged_at?: string
          occurrence_start: string
          offset_minutes: number
          schedule_id: string
          user_id?: string
        }
        Update: {
          acknowledged_at?: string
          occurrence_start?: string
          offset_minutes?: number
          schedule_id?: string
          user_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "economic_recurring_acknowledgements_schedule_id_fkey"
            columns: ["schedule_id"]
            isOneToOne: false
            referencedRelation: "economic_recurring_schedules"
            referencedColumns: ["id"]
          },
        ]
      }
      economic_recurring_schedules: {
        Row: {
          active: boolean
          created_at: string
          created_by: string
          days_of_week: number[]
          end_time: string | null
          id: string
          importance: number
          name: string
          reminder_offsets: number[]
          start_time: string
          timezone: string
          updated_at: string
          updated_by: string
        }
        Insert: {
          active?: boolean
          created_at?: string
          created_by?: string
          days_of_week?: number[]
          end_time?: string | null
          id?: string
          importance?: number
          name: string
          reminder_offsets?: number[]
          start_time: string
          timezone?: string
          updated_at?: string
          updated_by?: string
        }
        Update: {
          active?: boolean
          created_at?: string
          created_by?: string
          days_of_week?: number[]
          end_time?: string | null
          id?: string
          importance?: number
          name?: string
          reminder_offsets?: number[]
          start_time?: string
          timezone?: string
          updated_at?: string
          updated_by?: string
        }
        Relationships: []
      }
      economic_reminder_acknowledgements: {
        Row: {
          acknowledged_at: string
          reminder_id: string
          user_id: string
        }
        Insert: {
          acknowledged_at?: string
          reminder_id: string
          user_id?: string
        }
        Update: {
          acknowledged_at?: string
          reminder_id?: string
          user_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "economic_reminder_acknowledgements_reminder_id_fkey"
            columns: ["reminder_id"]
            isOneToOne: false
            referencedRelation: "economic_event_reminders"
            referencedColumns: ["id"]
          },
        ]
      }
      home_reminders: {
        Row: {
          body: string
          created_at: string
          frequency: string
          id: string
          updated_at: string
          user_id: string
        }
        Insert: {
          body?: string
          created_at?: string
          frequency?: string
          id?: string
          updated_at?: string
          user_id: string
        }
        Update: {
          body?: string
          created_at?: string
          frequency?: string
          id?: string
          updated_at?: string
          user_id?: string
        }
        Relationships: []
      }
      module_template_defaults: {
        Row: {
          created_at: string
          module_type: string
          symbol: string
          template_id: string
          updated_at: string
        }
        Insert: {
          created_at?: string
          module_type: string
          symbol?: string
          template_id: string
          updated_at?: string
        }
        Update: {
          created_at?: string
          module_type?: string
          symbol?: string
          template_id?: string
          updated_at?: string
        }
        Relationships: [
          {
            foreignKeyName: "module_template_defaults_template_id_fkey"
            columns: ["template_id"]
            isOneToOne: false
            referencedRelation: "templates"
            referencedColumns: ["id"]
          },
        ]
      }
      pinned_answers: {
        Row: {
          answer_snapshot: Json
          answer_updated_at: string | null
          author_label: string | null
          created_at: string
          id: string
          image_paths: Json
          pinned_by: string
          question_key: string
          question_title: string | null
          question_type: string
          source_group_id: string | null
          source_project_id: string | null
          source_project_name: string | null
          source_question_id: string | null
          source_stage_id: string | null
          source_trade_cycle_id: string | null
          updated_at: string
        }
        Insert: {
          answer_snapshot?: Json
          answer_updated_at?: string | null
          author_label?: string | null
          created_at?: string
          id?: string
          image_paths?: Json
          pinned_by?: string
          question_key: string
          question_title?: string | null
          question_type?: string
          source_group_id?: string | null
          source_project_id?: string | null
          source_project_name?: string | null
          source_question_id?: string | null
          source_stage_id?: string | null
          source_trade_cycle_id?: string | null
          updated_at?: string
        }
        Update: {
          answer_snapshot?: Json
          answer_updated_at?: string | null
          author_label?: string | null
          created_at?: string
          id?: string
          image_paths?: Json
          pinned_by?: string
          question_key?: string
          question_title?: string | null
          question_type?: string
          source_group_id?: string | null
          source_project_id?: string | null
          source_project_name?: string | null
          source_question_id?: string | null
          source_stage_id?: string | null
          source_trade_cycle_id?: string | null
          updated_at?: string
        }
        Relationships: [
          {
            foreignKeyName: "pinned_answers_source_trade_cycle_id_fkey"
            columns: ["source_trade_cycle_id"]
            isOneToOne: false
            referencedRelation: "project_trade_cycles"
            referencedColumns: ["id"]
          },
        ]
      }
      profiles: {
        Row: {
          created_at: string
          display_name: string | null
          email: string | null
          id: string
          updated_at: string
        }
        Insert: {
          created_at?: string
          display_name?: string | null
          email?: string | null
          id: string
          updated_at?: string
        }
        Update: {
          created_at?: string
          display_name?: string | null
          email?: string | null
          id?: string
          updated_at?: string
        }
        Relationships: []
      }
      project_activity: {
        Row: {
          actor_id: string | null
          created_at: string
          group_id: string | null
          id: string
          kind: string
          project_id: string
          question_id: string | null
          question_key: string | null
          stage_id: string | null
          trade_cycle_id: string
        }
        Insert: {
          actor_id?: string | null
          created_at?: string
          group_id?: string | null
          id?: string
          kind?: string
          project_id: string
          question_id?: string | null
          question_key?: string | null
          stage_id?: string | null
          trade_cycle_id: string
        }
        Update: {
          actor_id?: string | null
          created_at?: string
          group_id?: string | null
          id?: string
          kind?: string
          project_id?: string
          question_id?: string | null
          question_key?: string | null
          stage_id?: string | null
          trade_cycle_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "project_activity_project_id_fkey"
            columns: ["project_id"]
            isOneToOne: false
            referencedRelation: "projects"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "project_activity_trade_cycle_id_fkey"
            columns: ["trade_cycle_id"]
            isOneToOne: false
            referencedRelation: "project_trade_cycles"
            referencedColumns: ["id"]
          },
        ]
      }
      project_answers: {
        Row: {
          boolean_value: boolean | null
          break_even: boolean
          choice_value_json: Json | null
          created_at: string
          date_value: string | null
          id: string
          last_edited_by: string | null
          not_relevant: boolean
          number_value: number | null
          project_id: string
          question_id: string
          text_value: string | null
          trade_cycle_id: string
          updated_at: string
        }
        Insert: {
          boolean_value?: boolean | null
          break_even?: boolean
          choice_value_json?: Json | null
          created_at?: string
          date_value?: string | null
          id?: string
          last_edited_by?: string | null
          not_relevant?: boolean
          number_value?: number | null
          project_id: string
          question_id: string
          text_value?: string | null
          trade_cycle_id: string
          updated_at?: string
        }
        Update: {
          boolean_value?: boolean | null
          break_even?: boolean
          choice_value_json?: Json | null
          created_at?: string
          date_value?: string | null
          id?: string
          last_edited_by?: string | null
          not_relevant?: boolean
          number_value?: number | null
          project_id?: string
          question_id?: string
          text_value?: string | null
          trade_cycle_id?: string
          updated_at?: string
        }
        Relationships: [
          {
            foreignKeyName: "project_answers_project_id_fkey"
            columns: ["project_id"]
            isOneToOne: false
            referencedRelation: "projects"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "project_answers_question_id_fkey"
            columns: ["question_id"]
            isOneToOne: false
            referencedRelation: "template_questions"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "project_answers_trade_cycle_id_fkey"
            columns: ["trade_cycle_id"]
            isOneToOne: false
            referencedRelation: "project_trade_cycles"
            referencedColumns: ["id"]
          },
        ]
      }
      project_images: {
        Row: {
          caption: string | null
          created_at: string
          description: string | null
          file_name: string | null
          id: string
          mime_type: string | null
          original_storage_path: string | null
          project_id: string
          question_id: string | null
          storage_path: string
          trade_cycle_id: string
          updated_at: string
        }
        Insert: {
          caption?: string | null
          created_at?: string
          description?: string | null
          file_name?: string | null
          id?: string
          mime_type?: string | null
          original_storage_path?: string | null
          project_id: string
          question_id?: string | null
          storage_path: string
          trade_cycle_id: string
          updated_at?: string
        }
        Update: {
          caption?: string | null
          created_at?: string
          description?: string | null
          file_name?: string | null
          id?: string
          mime_type?: string | null
          original_storage_path?: string | null
          project_id?: string
          question_id?: string | null
          storage_path?: string
          trade_cycle_id?: string
          updated_at?: string
        }
        Relationships: [
          {
            foreignKeyName: "project_images_project_id_fkey"
            columns: ["project_id"]
            isOneToOne: false
            referencedRelation: "projects"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "project_images_question_id_fkey"
            columns: ["question_id"]
            isOneToOne: false
            referencedRelation: "template_questions"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "project_images_trade_cycle_id_fkey"
            columns: ["trade_cycle_id"]
            isOneToOne: false
            referencedRelation: "project_trade_cycles"
            referencedColumns: ["id"]
          },
        ]
      }
      project_liquidity_levels: {
        Row: {
          amount: string
          created_at: string
          id: string
          position: number
          price_text: string
          project_id: string
          row_id: string
          side: string
          taken: boolean
          updated_at: string
          updated_by: string | null
        }
        Insert: {
          amount?: string
          created_at?: string
          id?: string
          position?: number
          price_text?: string
          project_id: string
          row_id: string
          side: string
          taken?: boolean
          updated_at?: string
          updated_by?: string | null
        }
        Update: {
          amount?: string
          created_at?: string
          id?: string
          position?: number
          price_text?: string
          project_id?: string
          row_id?: string
          side?: string
          taken?: boolean
          updated_at?: string
          updated_by?: string | null
        }
        Relationships: [
          {
            foreignKeyName: "project_liquidity_levels_project_id_fkey"
            columns: ["project_id"]
            isOneToOne: false
            referencedRelation: "projects"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "project_liquidity_levels_row_id_fkey"
            columns: ["row_id"]
            isOneToOne: false
            referencedRelation: "project_liquidity_rows"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "project_liquidity_levels_row_project_fkey"
            columns: ["row_id", "project_id"]
            isOneToOne: false
            referencedRelation: "project_liquidity_rows"
            referencedColumns: ["id", "project_id"]
          },
        ]
      }
      project_liquidity_rows: {
        Row: {
          created_at: string
          id: string
          position: number
          project_id: string
          question_id: string
          timeframe: string
          updated_at: string
          updated_by: string | null
        }
        Insert: {
          created_at?: string
          id?: string
          position?: number
          project_id: string
          question_id: string
          timeframe: string
          updated_at?: string
          updated_by?: string | null
        }
        Update: {
          created_at?: string
          id?: string
          position?: number
          project_id?: string
          question_id?: string
          timeframe?: string
          updated_at?: string
          updated_by?: string | null
        }
        Relationships: [
          {
            foreignKeyName: "project_liquidity_rows_project_id_fkey"
            columns: ["project_id"]
            isOneToOne: false
            referencedRelation: "projects"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "project_liquidity_rows_question_id_fkey"
            columns: ["question_id"]
            isOneToOne: false
            referencedRelation: "template_questions"
            referencedColumns: ["id"]
          },
        ]
      }
      project_members: {
        Row: {
          created_at: string
          id: string
          project_id: string
          role: string
          user_id: string
        }
        Insert: {
          created_at?: string
          id?: string
          project_id: string
          role?: string
          user_id: string
        }
        Update: {
          created_at?: string
          id?: string
          project_id?: string
          role?: string
          user_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "project_members_project_id_fkey"
            columns: ["project_id"]
            isOneToOne: false
            referencedRelation: "projects"
            referencedColumns: ["id"]
          },
        ]
      }
      project_news_rows: {
        Row: {
          body: string
          created_at: string
          id: string
          importance: string
          position: number
          project_id: string
          question_id: string
          updated_at: string
          updated_by: string | null
        }
        Insert: {
          body?: string
          created_at?: string
          id?: string
          importance?: string
          position?: number
          project_id: string
          question_id: string
          updated_at?: string
          updated_by?: string | null
        }
        Update: {
          body?: string
          created_at?: string
          id?: string
          importance?: string
          position?: number
          project_id?: string
          question_id?: string
          updated_at?: string
          updated_by?: string | null
        }
        Relationships: [
          {
            foreignKeyName: "project_news_rows_project_id_fkey"
            columns: ["project_id"]
            isOneToOne: false
            referencedRelation: "projects"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "project_news_rows_question_id_fkey"
            columns: ["question_id"]
            isOneToOne: false
            referencedRelation: "template_questions"
            referencedColumns: ["id"]
          },
        ]
      }
      project_question_seen: {
        Row: {
          project_id: string
          question_id: string
          seen_at: string
          trade_cycle_id: string
          user_id: string
        }
        Insert: {
          project_id: string
          question_id: string
          seen_at?: string
          trade_cycle_id: string
          user_id?: string
        }
        Update: {
          project_id?: string
          question_id?: string
          seen_at?: string
          trade_cycle_id?: string
          user_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "project_question_seen_project_id_fkey"
            columns: ["project_id"]
            isOneToOne: false
            referencedRelation: "projects"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "project_question_seen_trade_cycle_id_fkey"
            columns: ["trade_cycle_id"]
            isOneToOne: false
            referencedRelation: "project_trade_cycles"
            referencedColumns: ["id"]
          },
        ]
      }
      project_trade_cycles: {
        Row: {
          created_at: string
          created_by: string | null
          cycle_number: number
          first_close_at: string | null
          first_pnl_at: string | null
          formula_name: string | null
          id: string
          project_id: string
        }
        Insert: {
          created_at?: string
          created_by?: string | null
          cycle_number: number
          first_close_at?: string | null
          first_pnl_at?: string | null
          formula_name?: string | null
          id?: string
          project_id: string
        }
        Update: {
          created_at?: string
          created_by?: string | null
          cycle_number?: number
          first_close_at?: string | null
          first_pnl_at?: string | null
          formula_name?: string | null
          id?: string
          project_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "project_trade_cycles_project_id_fkey"
            columns: ["project_id"]
            isOneToOne: false
            referencedRelation: "projects"
            referencedColumns: ["id"]
          },
        ]
      }
      project_user_state: {
        Row: {
          created_at: string
          first_opened_at: string | null
          last_stage_id: string | null
          last_trade_cycle_id: string | null
          project_id: string
          stage_seen: Json
          updated_at: string
          user_id: string
        }
        Insert: {
          created_at?: string
          first_opened_at?: string | null
          last_stage_id?: string | null
          last_trade_cycle_id?: string | null
          project_id: string
          stage_seen?: Json
          updated_at?: string
          user_id?: string
        }
        Update: {
          created_at?: string
          first_opened_at?: string | null
          last_stage_id?: string | null
          last_trade_cycle_id?: string | null
          project_id?: string
          stage_seen?: Json
          updated_at?: string
          user_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "project_user_state_last_trade_cycle_id_fkey"
            columns: ["last_trade_cycle_id"]
            isOneToOne: false
            referencedRelation: "project_trade_cycles"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "project_user_state_project_id_fkey"
            columns: ["project_id"]
            isOneToOne: false
            referencedRelation: "projects"
            referencedColumns: ["id"]
          },
        ]
      }
      projects: {
        Row: {
          archived_at: string | null
          completed_at: string | null
          created_at: string
          current_stage_id: string | null
          direction: string
          id: string
          initial_note: string | null
          intuition: string | null
          last_edited_by: string | null
          module_type: string
          name: string
          opportunity_source: string | null
          progress_percent: number
          scenario_ideas: string | null
          source_scenario_id: string | null
          status: string
          symbol: string | null
          template_version_id: string | null
          timeframe: string | null
          updated_at: string
          user_id: string | null
        }
        Insert: {
          archived_at?: string | null
          completed_at?: string | null
          created_at?: string
          current_stage_id?: string | null
          direction?: string
          id?: string
          initial_note?: string | null
          intuition?: string | null
          last_edited_by?: string | null
          module_type?: string
          name: string
          opportunity_source?: string | null
          progress_percent?: number
          scenario_ideas?: string | null
          source_scenario_id?: string | null
          status?: string
          symbol?: string | null
          template_version_id?: string | null
          timeframe?: string | null
          updated_at?: string
          user_id?: string | null
        }
        Update: {
          archived_at?: string | null
          completed_at?: string | null
          created_at?: string
          current_stage_id?: string | null
          direction?: string
          id?: string
          initial_note?: string | null
          intuition?: string | null
          last_edited_by?: string | null
          module_type?: string
          name?: string
          opportunity_source?: string | null
          progress_percent?: number
          scenario_ideas?: string | null
          source_scenario_id?: string | null
          status?: string
          symbol?: string | null
          template_version_id?: string | null
          timeframe?: string | null
          updated_at?: string
          user_id?: string | null
        }
        Relationships: [
          {
            foreignKeyName: "projects_source_scenario_id_fkey"
            columns: ["source_scenario_id"]
            isOneToOne: false
            referencedRelation: "scenarios"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "projects_template_version_id_fkey"
            columns: ["template_version_id"]
            isOneToOne: false
            referencedRelation: "template_versions"
            referencedColumns: ["id"]
          },
        ]
      }
      question_user_checks: {
        Row: {
          checked: boolean
          created_at: string
          id: string
          project_id: string
          question_id: string
          trade_cycle_id: string
          updated_at: string
          user_id: string
        }
        Insert: {
          checked?: boolean
          created_at?: string
          id?: string
          project_id: string
          question_id: string
          trade_cycle_id: string
          updated_at?: string
          user_id?: string
        }
        Update: {
          checked?: boolean
          created_at?: string
          id?: string
          project_id?: string
          question_id?: string
          trade_cycle_id?: string
          updated_at?: string
          user_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "question_user_checks_project_id_fkey"
            columns: ["project_id"]
            isOneToOne: false
            referencedRelation: "projects"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "question_user_checks_question_id_fkey"
            columns: ["question_id"]
            isOneToOne: false
            referencedRelation: "template_questions"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "question_user_checks_trade_cycle_id_fkey"
            columns: ["trade_cycle_id"]
            isOneToOne: false
            referencedRelation: "project_trade_cycles"
            referencedColumns: ["id"]
          },
        ]
      }
      scenario_assets: {
        Row: {
          active: boolean
          created_at: string
          created_by: string
          display_name: string
          id: string
          position: number
          symbol: string
          template_config: Json
          updated_at: string
        }
        Insert: {
          active?: boolean
          created_at?: string
          created_by?: string
          display_name: string
          id?: string
          position?: number
          symbol: string
          template_config?: Json
          updated_at?: string
        }
        Update: {
          active?: boolean
          created_at?: string
          created_by?: string
          display_name?: string
          id?: string
          position?: number
          symbol?: string
          template_config?: Json
          updated_at?: string
        }
        Relationships: []
      }
      scenario_variants: {
        Row: {
          created_at: string
          id: string
          invalidation: string
          label: string
          position: number
          reasons: string
          scenario_html: string
          scenario_id: string
          updated_at: string
        }
        Insert: {
          created_at?: string
          id?: string
          invalidation?: string
          label: string
          position?: number
          reasons?: string
          scenario_html?: string
          scenario_id: string
          updated_at?: string
        }
        Update: {
          created_at?: string
          id?: string
          invalidation?: string
          label?: string
          position?: number
          reasons?: string
          scenario_html?: string
          scenario_id?: string
          updated_at?: string
        }
        Relationships: [
          {
            foreignKeyName: "scenario_variants_scenario_id_fkey"
            columns: ["scenario_id"]
            isOneToOne: false
            referencedRelation: "scenarios"
            referencedColumns: ["id"]
          },
        ]
      }
      scenario_watch_links: {
        Row: {
          active: boolean
          asset_id: string
          category: string
          created_at: string
          id: string
          label: string
          position: number
          updated_at: string
          url: string
        }
        Insert: {
          active?: boolean
          asset_id: string
          category: string
          created_at?: string
          id?: string
          label: string
          position?: number
          updated_at?: string
          url: string
        }
        Update: {
          active?: boolean
          asset_id?: string
          category?: string
          created_at?: string
          id?: string
          label?: string
          position?: number
          updated_at?: string
          url?: string
        }
        Relationships: [
          {
            foreignKeyName: "scenario_watch_links_asset_id_fkey"
            columns: ["asset_id"]
            isOneToOne: false
            referencedRelation: "scenario_assets"
            referencedColumns: ["id"]
          },
        ]
      }
      scenarios: {
        Row: {
          asset_id: string
          author_text: string
          created_at: string
          created_by: string
          id: string
          pinned: boolean
          position: number
          status: string
          updated_at: string
          updated_by: string
        }
        Insert: {
          asset_id: string
          author_text?: string
          created_at?: string
          created_by?: string
          id?: string
          pinned?: boolean
          position?: number
          status?: string
          updated_at?: string
          updated_by?: string
        }
        Update: {
          asset_id?: string
          author_text?: string
          created_at?: string
          created_by?: string
          id?: string
          pinned?: boolean
          position?: number
          status?: string
          updated_at?: string
          updated_by?: string
        }
        Relationships: [
          {
            foreignKeyName: "scenarios_asset_id_fkey"
            columns: ["asset_id"]
            isOneToOne: false
            referencedRelation: "scenario_assets"
            referencedColumns: ["id"]
          },
        ]
      }
      spot_accounts: {
        Row: {
          account_type: string
          active: boolean
          address: string | null
          created_at: string
          created_by: string
          device: string | null
          id: string
          label: string
          notes: string | null
          updated_at: string
        }
        Insert: {
          account_type: string
          active?: boolean
          address?: string | null
          created_at?: string
          created_by?: string
          device?: string | null
          id?: string
          label: string
          notes?: string | null
          updated_at?: string
        }
        Update: {
          account_type?: string
          active?: boolean
          address?: string | null
          created_at?: string
          created_by?: string
          device?: string | null
          id?: string
          label?: string
          notes?: string | null
          updated_at?: string
        }
        Relationships: []
      }
      spot_alert_acknowledgements: {
        Row: {
          acknowledged_at: string
          alert_id: string
          triggered_price: number | null
          user_id: string
        }
        Insert: {
          acknowledged_at?: string
          alert_id: string
          triggered_price?: number | null
          user_id?: string
        }
        Update: {
          acknowledged_at?: string
          alert_id?: string
          triggered_price?: number | null
          user_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "spot_alert_acknowledgements_alert_id_fkey"
            columns: ["alert_id"]
            isOneToOne: false
            referencedRelation: "spot_price_alerts"
            referencedColumns: ["id"]
          },
        ]
      }
      spot_assets: {
        Row: {
          active: boolean
          category: string
          created_at: string
          display_name: string
          id: string
          position: number
          symbol: string
          updated_at: string
        }
        Insert: {
          active?: boolean
          category: string
          created_at?: string
          display_name: string
          id?: string
          position?: number
          symbol: string
          updated_at?: string
        }
        Update: {
          active?: boolean
          category?: string
          created_at?: string
          display_name?: string
          id?: string
          position?: number
          symbol?: string
          updated_at?: string
        }
        Relationships: []
      }
      spot_positions: {
        Row: {
          account_id: string | null
          asset_id: string
          created_at: string
          created_by: string
          exchange_orders: string | null
          id: string
          label: string
          notes: string | null
          status: string
          target_price: number | null
          updated_at: string
          updated_by: string
        }
        Insert: {
          account_id?: string | null
          asset_id: string
          created_at?: string
          created_by?: string
          exchange_orders?: string | null
          id?: string
          label?: string
          notes?: string | null
          status?: string
          target_price?: number | null
          updated_at?: string
          updated_by?: string
        }
        Update: {
          account_id?: string | null
          asset_id?: string
          created_at?: string
          created_by?: string
          exchange_orders?: string | null
          id?: string
          label?: string
          notes?: string | null
          status?: string
          target_price?: number | null
          updated_at?: string
          updated_by?: string
        }
        Relationships: [
          {
            foreignKeyName: "spot_positions_account_id_fkey"
            columns: ["account_id"]
            isOneToOne: false
            referencedRelation: "spot_accounts"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "spot_positions_asset_id_fkey"
            columns: ["asset_id"]
            isOneToOne: false
            referencedRelation: "spot_assets"
            referencedColumns: ["id"]
          },
        ]
      }
      spot_price_alerts: {
        Row: {
          active: boolean
          created_at: string
          created_by: string
          direction: string
          id: string
          position_id: string
          target_price: number
          updated_at: string
        }
        Insert: {
          active?: boolean
          created_at?: string
          created_by?: string
          direction?: string
          id?: string
          position_id: string
          target_price: number
          updated_at?: string
        }
        Update: {
          active?: boolean
          created_at?: string
          created_by?: string
          direction?: string
          id?: string
          position_id?: string
          target_price?: number
          updated_at?: string
        }
        Relationships: [
          {
            foreignKeyName: "spot_price_alerts_position_id_fkey"
            columns: ["position_id"]
            isOneToOne: false
            referencedRelation: "spot_positions"
            referencedColumns: ["id"]
          },
        ]
      }
      spot_transactions: {
        Row: {
          created_at: string
          created_by: string
          fee: number
          id: string
          notes: string | null
          occurred_at: string
          position_id: string
          price: number | null
          quantity: number
          transaction_type: string
          updated_at: string
        }
        Insert: {
          created_at?: string
          created_by?: string
          fee?: number
          id?: string
          notes?: string | null
          occurred_at?: string
          position_id: string
          price?: number | null
          quantity: number
          transaction_type: string
          updated_at?: string
        }
        Update: {
          created_at?: string
          created_by?: string
          fee?: number
          id?: string
          notes?: string | null
          occurred_at?: string
          position_id?: string
          price?: number | null
          quantity?: number
          transaction_type?: string
          updated_at?: string
        }
        Relationships: [
          {
            foreignKeyName: "spot_transactions_position_id_fkey"
            columns: ["position_id"]
            isOneToOne: false
            referencedRelation: "spot_positions"
            referencedColumns: ["id"]
          },
        ]
      }
      template_groups: {
        Row: {
          active: boolean
          behavior: string | null
          collapsible: boolean
          description: string | null
          id: string
          position: number
          stage_id: string
          title: string
        }
        Insert: {
          active?: boolean
          behavior?: string | null
          collapsible?: boolean
          description?: string | null
          id?: string
          position?: number
          stage_id: string
          title: string
        }
        Update: {
          active?: boolean
          behavior?: string | null
          collapsible?: boolean
          description?: string | null
          id?: string
          position?: number
          stage_id?: string
          title?: string
        }
        Relationships: [
          {
            foreignKeyName: "template_groups_stage_id_fkey"
            columns: ["stage_id"]
            isOneToOne: false
            referencedRelation: "template_stages"
            referencedColumns: ["id"]
          },
        ]
      }
      template_questions: {
        Row: {
          active: boolean
          default_text_align: string
          description: string | null
          field_key: string | null
          group_id: string
          help_text: string | null
          id: string
          links_json: Json
          options_json: Json | null
          pinned_scope: string
          pinned_stage_id: string | null
          position: number
          question_key: string | null
          question_type: string
          required: boolean
          source_url: string | null
          title: string
          visibility_rule_json: Json | null
        }
        Insert: {
          active?: boolean
          default_text_align?: string
          description?: string | null
          field_key?: string | null
          group_id: string
          help_text?: string | null
          id?: string
          links_json?: Json
          options_json?: Json | null
          pinned_scope?: string
          pinned_stage_id?: string | null
          position?: number
          question_key?: string | null
          question_type?: string
          required?: boolean
          source_url?: string | null
          title: string
          visibility_rule_json?: Json | null
        }
        Update: {
          active?: boolean
          default_text_align?: string
          description?: string | null
          field_key?: string | null
          group_id?: string
          help_text?: string | null
          id?: string
          links_json?: Json
          options_json?: Json | null
          pinned_scope?: string
          pinned_stage_id?: string | null
          position?: number
          question_key?: string | null
          question_type?: string
          required?: boolean
          source_url?: string | null
          title?: string
          visibility_rule_json?: Json | null
        }
        Relationships: [
          {
            foreignKeyName: "template_questions_group_id_fkey"
            columns: ["group_id"]
            isOneToOne: false
            referencedRelation: "template_groups"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "template_questions_pinned_stage_id_fkey"
            columns: ["pinned_stage_id"]
            isOneToOne: false
            referencedRelation: "template_stages"
            referencedColumns: ["id"]
          },
        ]
      }
      template_stages: {
        Row: {
          active: boolean
          description: string | null
          id: string
          position: number
          template_version_id: string
          title: string
        }
        Insert: {
          active?: boolean
          description?: string | null
          id?: string
          position?: number
          template_version_id: string
          title: string
        }
        Update: {
          active?: boolean
          description?: string | null
          id?: string
          position?: number
          template_version_id?: string
          title?: string
        }
        Relationships: [
          {
            foreignKeyName: "template_stages_template_version_id_fkey"
            columns: ["template_version_id"]
            isOneToOne: false
            referencedRelation: "template_versions"
            referencedColumns: ["id"]
          },
        ]
      }
      template_versions: {
        Row: {
          created_at: string
          id: string
          published_at: string | null
          status: string
          template_id: string
          version_number: number
        }
        Insert: {
          created_at?: string
          id?: string
          published_at?: string | null
          status?: string
          template_id: string
          version_number?: number
        }
        Update: {
          created_at?: string
          id?: string
          published_at?: string | null
          status?: string
          template_id?: string
          version_number?: number
        }
        Relationships: [
          {
            foreignKeyName: "template_versions_template_id_fkey"
            columns: ["template_id"]
            isOneToOne: false
            referencedRelation: "templates"
            referencedColumns: ["id"]
          },
        ]
      }
      templates: {
        Row: {
          created_at: string
          description: string | null
          id: string
          kind: string
          name: string
          settings_json: Json
          slug: string | null
          updated_at: string
        }
        Insert: {
          created_at?: string
          description?: string | null
          id?: string
          kind?: string
          name: string
          settings_json?: Json
          slug?: string | null
          updated_at?: string
        }
        Update: {
          created_at?: string
          description?: string | null
          id?: string
          kind?: string
          name?: string
          settings_json?: Json
          slug?: string | null
          updated_at?: string
        }
        Relationships: []
      }
      trades: {
        Row: {
          actual_entry: number | null
          closed_at: string | null
          created_at: string
          direction: string | null
          exit_price: number | null
          id: string
          opened_at: string | null
          planned_entry: number | null
          pnl_amount: number | null
          pnl_percent: number | null
          position_size: number | null
          project_id: string
          result: string | null
          stop_loss: number | null
          summary: string | null
          symbol: string | null
          take_profit_json: Json | null
          trade_cycle_id: string
          updated_at: string
        }
        Insert: {
          actual_entry?: number | null
          closed_at?: string | null
          created_at?: string
          direction?: string | null
          exit_price?: number | null
          id?: string
          opened_at?: string | null
          planned_entry?: number | null
          pnl_amount?: number | null
          pnl_percent?: number | null
          position_size?: number | null
          project_id: string
          result?: string | null
          stop_loss?: number | null
          summary?: string | null
          symbol?: string | null
          take_profit_json?: Json | null
          trade_cycle_id: string
          updated_at?: string
        }
        Update: {
          actual_entry?: number | null
          closed_at?: string | null
          created_at?: string
          direction?: string | null
          exit_price?: number | null
          id?: string
          opened_at?: string | null
          planned_entry?: number | null
          pnl_amount?: number | null
          pnl_percent?: number | null
          position_size?: number | null
          project_id?: string
          result?: string | null
          stop_loss?: number | null
          summary?: string | null
          symbol?: string | null
          take_profit_json?: Json | null
          trade_cycle_id?: string
          updated_at?: string
        }
        Relationships: [
          {
            foreignKeyName: "trades_project_id_fkey"
            columns: ["project_id"]
            isOneToOne: false
            referencedRelation: "projects"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "trades_trade_cycle_id_fkey"
            columns: ["trade_cycle_id"]
            isOneToOne: false
            referencedRelation: "project_trade_cycles"
            referencedColumns: ["id"]
          },
        ]
      }
      user_preferences: {
        Row: {
          collection_news_language: string | null
          collection_news_sort: string
          created_at: string
          default_text_class: string
          updated_at: string
          user_id: string
        }
        Insert: {
          collection_news_language?: string | null
          collection_news_sort?: string
          created_at?: string
          default_text_class?: string
          updated_at?: string
          user_id: string
        }
        Update: {
          collection_news_language?: string | null
          collection_news_sort?: string
          created_at?: string
          default_text_class?: string
          updated_at?: string
          user_id?: string
        }
        Relationships: []
      }
      user_roles: {
        Row: {
          created_at: string
          id: string
          role: Database["public"]["Enums"]["app_role"]
          user_id: string
        }
        Insert: {
          created_at?: string
          id?: string
          role: Database["public"]["Enums"]["app_role"]
          user_id: string
        }
        Update: {
          created_at?: string
          id?: string
          role?: Database["public"]["Enums"]["app_role"]
          user_id?: string
        }
        Relationships: []
      }
      v2_ai_run_leases: {
        Row: {
          expires_at: string
          scope_key: string
          started_at: string
          user_id: string
          workspace_id: string
        }
        Insert: {
          expires_at: string
          scope_key: string
          started_at?: string
          user_id: string
          workspace_id: string
        }
        Update: {
          expires_at?: string
          scope_key?: string
          started_at?: string
          user_id?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_ai_run_leases_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_asset_settings: {
        Row: {
          active: boolean
          created_at: string
          created_by: string | null
          default_collection_template_id: string | null
          default_scenario_template_id: string | null
          default_trade_template_id: string | null
          display_name: string | null
          id: string
          settings: Json
          symbol: string
          updated_at: string
          updated_by: string | null
          watch_links: Json
          workspace_id: string
        }
        Insert: {
          active?: boolean
          created_at?: string
          created_by?: string | null
          default_collection_template_id?: string | null
          default_scenario_template_id?: string | null
          default_trade_template_id?: string | null
          display_name?: string | null
          id?: string
          settings?: Json
          symbol: string
          updated_at?: string
          updated_by?: string | null
          watch_links?: Json
          workspace_id: string
        }
        Update: {
          active?: boolean
          created_at?: string
          created_by?: string | null
          default_collection_template_id?: string | null
          default_scenario_template_id?: string | null
          default_trade_template_id?: string | null
          display_name?: string | null
          id?: string
          settings?: Json
          symbol?: string
          updated_at?: string
          updated_by?: string | null
          watch_links?: Json
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_asset_settings_default_collection_template_id_fkey"
            columns: ["default_collection_template_id"]
            isOneToOne: false
            referencedRelation: "v2_templates"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_asset_settings_default_scenario_template_id_fkey"
            columns: ["default_scenario_template_id"]
            isOneToOne: false
            referencedRelation: "v2_templates"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_asset_settings_default_trade_template_id_fkey"
            columns: ["default_trade_template_id"]
            isOneToOne: false
            referencedRelation: "v2_templates"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_asset_settings_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_automated_trade_cards: {
        Row: {
          card_id: string
          created_at: string
          observed_at: string
          payload: Json
          revision: number
          updated_at: string
          workspace_id: string
        }
        Insert: {
          card_id: string
          created_at?: string
          observed_at: string
          payload?: Json
          revision?: number
          updated_at?: string
          workspace_id: string
        }
        Update: {
          card_id?: string
          created_at?: string
          observed_at?: string
          payload?: Json
          revision?: number
          updated_at?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_automated_trade_cards_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_autofill_items: {
        Row: {
          item_key: string
          logical_key: string
          published_at: string
          sheet_key: string
          source_url: string
          workspace_id: string
        }
        Insert: {
          item_key: string
          logical_key: string
          published_at: string
          sheet_key: string
          source_url: string
          workspace_id: string
        }
        Update: {
          item_key?: string
          logical_key?: string
          published_at?: string
          sheet_key?: string
          source_url?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_autofill_items_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_autofill_publication_coverage: {
        Row: {
          from_at: string
          logical_key: string
          sheet_key: string
          source_fingerprint: string
          source_url: string
          to_at: string
          workspace_id: string
        }
        Insert: {
          from_at: string
          logical_key: string
          sheet_key: string
          source_fingerprint: string
          source_url: string
          to_at: string
          workspace_id: string
        }
        Update: {
          from_at?: string
          logical_key?: string
          sheet_key?: string
          source_fingerprint?: string
          source_url?: string
          to_at?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_autofill_publication_coverage_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_autofill_publication_profiles: {
        Row: {
          evidence_revision: string
          logical_key: string
          model: Json
          model_revision: string | null
          sheet_key: string
          source_fingerprint: string
          updated_at: string
          workspace_id: string
        }
        Insert: {
          evidence_revision?: string
          logical_key: string
          model?: Json
          model_revision?: string | null
          sheet_key: string
          source_fingerprint: string
          updated_at?: string
          workspace_id: string
        }
        Update: {
          evidence_revision?: string
          logical_key?: string
          model?: Json
          model_revision?: string | null
          sheet_key?: string
          source_fingerprint?: string
          updated_at?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_autofill_publication_profiles_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_autofill_publications: {
        Row: {
          item_key: string
          item_url: string
          logical_key: string
          observed_at: string
          published_at: string
          sheet_key: string
          source_fingerprint: string
          source_url: string
          workspace_id: string
        }
        Insert: {
          item_key: string
          item_url: string
          logical_key: string
          observed_at?: string
          published_at: string
          sheet_key: string
          source_fingerprint: string
          source_url: string
          workspace_id: string
        }
        Update: {
          item_key?: string
          item_url?: string
          logical_key?: string
          observed_at?: string
          published_at?: string
          sheet_key?: string
          source_fingerprint?: string
          source_url?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_autofill_publications_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_autofill_runs: {
        Row: {
          automation_config_key: string | null
          baseline: Json
          baseline_version: number
          count: number
          created_at: string
          field_id: string
          finished_at: string | null
          id: string
          logical_key: string
          manual: boolean
          message: string | null
          session_id: string | null
          sheet_key: string
          status: string
          template_revision: number
          workspace_id: string
        }
        Insert: {
          automation_config_key?: string | null
          baseline?: Json
          baseline_version?: number
          count?: number
          created_at?: string
          field_id: string
          finished_at?: string | null
          id?: string
          logical_key: string
          manual: boolean
          message?: string | null
          session_id?: string | null
          sheet_key: string
          status?: string
          template_revision: number
          workspace_id: string
        }
        Update: {
          automation_config_key?: string | null
          baseline?: Json
          baseline_version?: number
          count?: number
          created_at?: string
          field_id?: string
          finished_at?: string | null
          id?: string
          logical_key?: string
          manual?: boolean
          message?: string | null
          session_id?: string | null
          sheet_key?: string
          status?: string
          template_revision?: number
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_autofill_runs_field_id_fkey"
            columns: ["field_id"]
            isOneToOne: false
            referencedRelation: "v2_template_fields"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_collection_autofill_runs_session_id_fkey"
            columns: ["session_id"]
            isOneToOne: false
            referencedRelation: "v2_collection_sessions"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_collection_autofill_runs_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_autofill_state: {
        Row: {
          calendar_schedule_key: string | null
          last_scanned_at: string | null
          lease_id: string | null
          lease_until: string | null
          logical_key: string
          next_due_at: string
          sheet_key: string
          source_cursors: Json
          workspace_id: string
        }
        Insert: {
          calendar_schedule_key?: string | null
          last_scanned_at?: string | null
          lease_id?: string | null
          lease_until?: string | null
          logical_key: string
          next_due_at?: string
          sheet_key: string
          source_cursors?: Json
          workspace_id: string
        }
        Update: {
          calendar_schedule_key?: string | null
          last_scanned_at?: string | null
          lease_id?: string | null
          lease_until?: string | null
          logical_key?: string
          next_due_at?: string
          sheet_key?: string
          source_cursors?: Json
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_autofill_state_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_autofill_write_tokens: {
        Row: {
          run_id: string
          transaction_id: number
        }
        Insert: {
          run_id: string
          transaction_id: number
        }
        Update: {
          run_id?: string
          transaction_id?: number
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_autofill_write_tokens_run_id_fkey"
            columns: ["run_id"]
            isOneToOne: false
            referencedRelation: "v2_collection_autofill_runs"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_clear_history: {
        Row: {
          actor_id: string | null
          created_at: string
          field_id: string
          id: string
          logical_key: string
          operation_id: string
          previous_value: Json
          session_id: string
          workspace_id: string
        }
        Insert: {
          actor_id?: string | null
          created_at?: string
          field_id: string
          id?: string
          logical_key: string
          operation_id: string
          previous_value: Json
          session_id: string
          workspace_id: string
        }
        Update: {
          actor_id?: string | null
          created_at?: string
          field_id?: string
          id?: string
          logical_key?: string
          operation_id?: string
          previous_value?: Json
          session_id?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_clear_history_field_id_fkey"
            columns: ["field_id"]
            isOneToOne: false
            referencedRelation: "v2_template_fields"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_collection_clear_history_session_id_fkey"
            columns: ["session_id"]
            isOneToOne: false
            referencedRelation: "v2_collection_sessions"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_collection_clear_history_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_clear_tokens: {
        Row: {
          actor_id: string
          expected_value: Json
          field_id: string
          session_id: string
          transaction_id: number
          workspace_id: string
        }
        Insert: {
          actor_id: string
          expected_value: Json
          field_id: string
          session_id: string
          transaction_id: number
          workspace_id: string
        }
        Update: {
          actor_id?: string
          expected_value?: Json
          field_id?: string
          session_id?: string
          transaction_id?: number
          workspace_id?: string
        }
        Relationships: []
      }
      v2_collection_expiry_cohorts: {
        Row: {
          captured_after_expiry: boolean
          captured_at: string
          exchanges: Json
          expired_at: string | null
          expiry_date: string
          first_captured_at: string
          id: string
          logical_key: string
          origin: string
          selected: boolean
          source_url: string | null
          workspace_id: string
        }
        Insert: {
          captured_after_expiry?: boolean
          captured_at: string
          exchanges: Json
          expired_at?: string | null
          expiry_date: string
          first_captured_at?: string
          id?: string
          logical_key: string
          origin: string
          selected?: boolean
          source_url?: string | null
          workspace_id: string
        }
        Update: {
          captured_after_expiry?: boolean
          captured_at?: string
          exchanges?: Json
          expired_at?: string | null
          expiry_date?: string
          first_captured_at?: string
          id?: string
          logical_key?: string
          origin?: string
          selected?: boolean
          source_url?: string | null
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_expiry_cohorts_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_expiry_maintenance_tokens: {
        Row: {
          scope: string
          transaction_id: number
          value_id: string
        }
        Insert: {
          scope: string
          transaction_id: number
          value_id: string
        }
        Update: {
          scope?: string
          transaction_id?: number
          value_id?: string
        }
        Relationships: []
      }
      v2_collection_expiry_market_days: {
        Row: {
          attempts: number
          captured_at: string | null
          close: number | null
          closed_at: string | null
          day: string
          high: number | null
          lease_id: string | null
          lease_until: string | null
          low: number | null
          next_attempt_at: string
          open: number | null
          opened_at: string | null
          source_url: string | null
          status: string
          volume_btc: number | null
        }
        Insert: {
          attempts?: number
          captured_at?: string | null
          close?: number | null
          closed_at?: string | null
          day: string
          high?: number | null
          lease_id?: string | null
          lease_until?: string | null
          low?: number | null
          next_attempt_at?: string
          open?: number | null
          opened_at?: string | null
          source_url?: string | null
          status?: string
          volume_btc?: number | null
        }
        Update: {
          attempts?: number
          captured_at?: string | null
          close?: number | null
          closed_at?: string | null
          day?: string
          high?: number | null
          lease_id?: string | null
          lease_until?: string | null
          low?: number | null
          next_attempt_at?: string
          open?: number | null
          opened_at?: string | null
          source_url?: string | null
          status?: string
          volume_btc?: number | null
        }
        Relationships: []
      }
      v2_collection_expiry_observations: {
        Row: {
          captured_after_expiry: boolean
          captured_at: string
          created_at: string
          exchanges: Json
          expiry_date: string
          fingerprint: string
          id: string
          logical_key: string
          origin: string
          selected: boolean
          source_url: string | null
          workspace_id: string
        }
        Insert: {
          captured_after_expiry: boolean
          captured_at: string
          created_at?: string
          exchanges: Json
          expiry_date: string
          fingerprint: string
          id?: string
          logical_key: string
          origin: string
          selected: boolean
          source_url?: string | null
          workspace_id: string
        }
        Update: {
          captured_after_expiry?: boolean
          captured_at?: string
          created_at?: string
          exchanges?: Json
          expiry_date?: string
          fingerprint?: string
          id?: string
          logical_key?: string
          origin?: string
          selected?: boolean
          source_url?: string | null
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_expiry_observations_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_field_activity: {
        Row: {
          actor_id: string | null
          actor_origin: string
          changed_at: string
          field_id: string
          logical_key: string | null
          news_row_ids: string[] | null
          session_id: string
          sheet_key: string | null
          version: number
          workspace_id: string | null
        }
        Insert: {
          actor_id?: string | null
          actor_origin?: string
          changed_at?: string
          field_id: string
          logical_key?: string | null
          news_row_ids?: string[] | null
          session_id: string
          sheet_key?: string | null
          version?: never
          workspace_id?: string | null
        }
        Update: {
          actor_id?: string | null
          actor_origin?: string
          changed_at?: string
          field_id?: string
          logical_key?: string | null
          news_row_ids?: string[] | null
          session_id?: string
          sheet_key?: string | null
          version?: never
          workspace_id?: string | null
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_field_activity_field_id_fkey"
            columns: ["field_id"]
            isOneToOne: false
            referencedRelation: "v2_template_fields"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_collection_field_activity_session_id_fkey"
            columns: ["session_id"]
            isOneToOne: false
            referencedRelation: "v2_collection_sessions"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_collection_field_activity_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_field_seen: {
        Row: {
          field_id: string
          seen_version: number
          session_id: string
          user_id: string
        }
        Insert: {
          field_id: string
          seen_version: number
          session_id: string
          user_id: string
        }
        Update: {
          field_id?: string
          seen_version?: number
          session_id?: string
          user_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_field_seen_field_id_fkey"
            columns: ["field_id"]
            isOneToOne: false
            referencedRelation: "v2_template_fields"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_collection_field_seen_session_id_fkey"
            columns: ["session_id"]
            isOneToOne: false
            referencedRelation: "v2_collection_sessions"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_live_seen: {
        Row: {
          logical_key: string
          seen_version: number
          user_id: string
          workspace_id: string
        }
        Insert: {
          logical_key: string
          seen_version?: number
          user_id: string
          workspace_id: string
        }
        Update: {
          logical_key?: string
          seen_version?: number
          user_id?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_live_seen_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_live_values: {
        Row: {
          created_at: string
          id: string
          logical_key: string
          updated_at: string
          updated_by: string | null
          updated_origin: string
          value: Json
          workspace_id: string
        }
        Insert: {
          created_at?: string
          id?: string
          logical_key: string
          updated_at?: string
          updated_by?: string | null
          updated_origin?: string
          value?: Json
          workspace_id: string
        }
        Update: {
          created_at?: string
          id?: string
          logical_key?: string
          updated_at?: string
          updated_by?: string | null
          updated_origin?: string
          value?: Json
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_live_values_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_news_assessments: {
        Row: {
          body: string
          created_at: string
          item_key: string
          learning_source_url: string
          logical_key: string
          model_evaluation: Json | null
          published_at: string
          run_id: string | null
          sheet_key: string
          source_name: string | null
          source_url: string
          workspace_id: string
        }
        Insert: {
          body: string
          created_at?: string
          item_key: string
          learning_source_url: string
          logical_key: string
          model_evaluation?: Json | null
          published_at: string
          run_id?: string | null
          sheet_key: string
          source_name?: string | null
          source_url: string
          workspace_id: string
        }
        Update: {
          body?: string
          created_at?: string
          item_key?: string
          learning_source_url?: string
          logical_key?: string
          model_evaluation?: Json | null
          published_at?: string
          run_id?: string | null
          sheet_key?: string
          source_name?: string | null
          source_url?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_news_assessments_run_id_fkey"
            columns: ["run_id"]
            isOneToOne: false
            referencedRelation: "v2_collection_autofill_runs"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_collection_news_assessments_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_news_feedback: {
        Row: {
          action: string
          actor_id: string
          body: string
          created_at: string
          detail: string | null
          feedback_level: string | null
          field_id: string
          item_key: string
          learning_source_url: string
          logical_key: string
          model_evaluation: Json | null
          row_id: string
          score: number | null
          sentiment: string | null
          session_id: string
          sheet_key: string
          source_name: string | null
          source_url: string
          updated_at: string
          workspace_id: string
        }
        Insert: {
          action: string
          actor_id: string
          body: string
          created_at?: string
          detail?: string | null
          feedback_level?: string | null
          field_id: string
          item_key: string
          learning_source_url: string
          logical_key: string
          model_evaluation?: Json | null
          row_id: string
          score?: number | null
          sentiment?: string | null
          session_id: string
          sheet_key: string
          source_name?: string | null
          source_url: string
          updated_at?: string
          workspace_id: string
        }
        Update: {
          action?: string
          actor_id?: string
          body?: string
          created_at?: string
          detail?: string | null
          feedback_level?: string | null
          field_id?: string
          item_key?: string
          learning_source_url?: string
          logical_key?: string
          model_evaluation?: Json | null
          row_id?: string
          score?: number | null
          sentiment?: string | null
          session_id?: string
          sheet_key?: string
          source_name?: string | null
          source_url?: string
          updated_at?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_news_feedback_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_news_feedback_bootstrap: {
        Row: {
          completed_at: string
          singleton: boolean
        }
        Insert: {
          completed_at?: string
          singleton?: boolean
        }
        Update: {
          completed_at?: string
          singleton?: boolean
        }
        Relationships: []
      }
      v2_collection_news_feedback_events: {
        Row: {
          actor_id: string | null
          event: Json
          id: string
          item_key: string
          occurred_at: string
          workspace_id: string
        }
        Insert: {
          actor_id?: string | null
          event: Json
          id?: string
          item_key: string
          occurred_at?: string
          workspace_id: string
        }
        Update: {
          actor_id?: string | null
          event?: Json
          id?: string
          item_key?: string
          occurred_at?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_news_feedback_events_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_news_orders: {
        Row: {
          field_id: string
          revision: number
          row_ids: string[]
          session_id: string
          updated_at: string
          updated_by: string | null
          workspace_id: string
        }
        Insert: {
          field_id: string
          revision?: number
          row_ids?: string[]
          session_id: string
          updated_at?: string
          updated_by?: string | null
          workspace_id: string
        }
        Update: {
          field_id?: string
          revision?: number
          row_ids?: string[]
          session_id?: string
          updated_at?: string
          updated_by?: string | null
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_news_orders_field_id_fkey"
            columns: ["field_id"]
            isOneToOne: false
            referencedRelation: "v2_template_fields"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_collection_news_orders_session_id_fkey"
            columns: ["session_id"]
            isOneToOne: false
            referencedRelation: "v2_collection_sessions"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_collection_news_orders_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_news_provenance: {
        Row: {
          created_at: string
          field_id: string
          item_key: string
          logical_key: string
          published_at: string
          row_id: string
          session_id: string
          sheet_key: string
          source_url: string
          storage: string
          workspace_id: string
        }
        Insert: {
          created_at?: string
          field_id: string
          item_key: string
          logical_key: string
          published_at: string
          row_id: string
          session_id: string
          sheet_key: string
          source_url: string
          storage: string
          workspace_id: string
        }
        Update: {
          created_at?: string
          field_id?: string
          item_key?: string
          logical_key?: string
          published_at?: string
          row_id?: string
          session_id?: string
          sheet_key?: string
          source_url?: string
          storage?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_news_provenance_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_news_read_markers: {
        Row: {
          logical_key: string
          read_until: string
          updated_at: string
          user_id: string
          workspace_id: string
        }
        Insert: {
          logical_key: string
          read_until: string
          updated_at?: string
          user_id: string
          workspace_id: string
        }
        Update: {
          logical_key?: string
          read_until?: string
          updated_at?: string
          user_id?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_news_read_markers_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_news_recycle_bin: {
        Row: {
          body: string
          deleted_at: string
          deleted_by: string | null
          detail: string | null
          field_id: string
          id: string
          importance: string
          item_key: string
          learning_source_url: string
          logical_key: string
          model_evaluation: Json | null
          reason: string
          restored_at: string | null
          restored_by: string | null
          row_id: string
          session_id: string
          sheet_key: string
          snapshot: Json
          source_name: string | null
          source_url: string
          storage: string
          workspace_id: string
        }
        Insert: {
          body: string
          deleted_at?: string
          deleted_by?: string | null
          detail?: string | null
          field_id: string
          id?: string
          importance: string
          item_key: string
          learning_source_url: string
          logical_key: string
          model_evaluation?: Json | null
          reason: string
          restored_at?: string | null
          restored_by?: string | null
          row_id: string
          session_id: string
          sheet_key: string
          snapshot: Json
          source_name?: string | null
          source_url: string
          storage: string
          workspace_id: string
        }
        Update: {
          body?: string
          deleted_at?: string
          deleted_by?: string | null
          detail?: string | null
          field_id?: string
          id?: string
          importance?: string
          item_key?: string
          learning_source_url?: string
          logical_key?: string
          model_evaluation?: Json | null
          reason?: string
          restored_at?: string | null
          restored_by?: string | null
          row_id?: string
          session_id?: string
          sheet_key?: string
          snapshot?: Json
          source_name?: string | null
          source_url?: string
          storage?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_news_recycle_bin_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_news_seen: {
        Row: {
          field_id: string
          row_id: string
          seen_version: number
          session_id: string
          user_id: string
        }
        Insert: {
          field_id: string
          row_id: string
          seen_version: number
          session_id: string
          user_id: string
        }
        Update: {
          field_id?: string
          row_id?: string
          seen_version?: number
          session_id?: string
          user_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_news_seen_field_id_fkey"
            columns: ["field_id"]
            isOneToOne: false
            referencedRelation: "v2_template_fields"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_collection_news_seen_session_id_fkey"
            columns: ["session_id"]
            isOneToOne: false
            referencedRelation: "v2_collection_sessions"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_news_source_claims: {
        Row: {
          first_run_id: string | null
          item_key: string
          logical_key: string
          published_at: string
          sheet_key: string
          source_identity: string
          source_url: string
          workspace_id: string
        }
        Insert: {
          first_run_id?: string | null
          item_key: string
          logical_key: string
          published_at: string
          sheet_key: string
          source_identity: string
          source_url: string
          workspace_id: string
        }
        Update: {
          first_run_id?: string | null
          item_key?: string
          logical_key?: string
          published_at?: string
          sheet_key?: string
          source_identity?: string
          source_url?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_news_source_claims_first_run_id_fkey"
            columns: ["first_run_id"]
            isOneToOne: false
            referencedRelation: "v2_collection_autofill_runs"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_collection_news_source_claims_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_previous_answer_pins: {
        Row: {
          field_keys: string[]
          id: string
          logical_key: string
          pinned_at: string
          pinned_by: string | null
          row_order: Json
          sheet_key: string
          snapshot: Json
          source_key: string
          source_selector: Json | null
          source_title: string
          workspace_id: string
        }
        Insert: {
          field_keys: string[]
          id?: string
          logical_key: string
          pinned_at?: string
          pinned_by?: string | null
          row_order?: Json
          sheet_key: string
          snapshot: Json
          source_key: string
          source_selector?: Json | null
          source_title: string
          workspace_id: string
        }
        Update: {
          field_keys?: string[]
          id?: string
          logical_key?: string
          pinned_at?: string
          pinned_by?: string | null
          row_order?: Json
          sheet_key?: string
          snapshot?: Json
          source_key?: string
          source_selector?: Json | null
          source_title?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_previous_answer_pins_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_scenario_links: {
        Row: {
          created_at: string
          created_by: string | null
          creation_request_id: string | null
          id: string
          position: number
          scenario_id: string
          session_id: string
          workspace_id: string
        }
        Insert: {
          created_at?: string
          created_by?: string | null
          creation_request_id?: string | null
          id?: string
          position: number
          scenario_id: string
          session_id: string
          workspace_id: string
        }
        Update: {
          created_at?: string
          created_by?: string | null
          creation_request_id?: string | null
          id?: string
          position?: number
          scenario_id?: string
          session_id?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_scenario_links_scenario_id_fkey"
            columns: ["scenario_id"]
            isOneToOne: false
            referencedRelation: "v2_scenarios"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_collection_scenario_links_session_id_fkey"
            columns: ["session_id"]
            isOneToOne: false
            referencedRelation: "v2_collection_sessions"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_collection_scenario_links_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_sessions: {
        Row: {
          created_at: string
          created_by: string
          creation_origin: string
          deleted_at: string | null
          deleted_by: string | null
          id: string
          last_edited_by: string | null
          status: string
          template_id: string | null
          title: string
          updated_at: string
          workspace_id: string
        }
        Insert: {
          created_at?: string
          created_by: string
          creation_origin?: string
          deleted_at?: string | null
          deleted_by?: string | null
          id?: string
          last_edited_by?: string | null
          status?: string
          template_id?: string | null
          title: string
          updated_at?: string
          workspace_id: string
        }
        Update: {
          created_at?: string
          created_by?: string
          creation_origin?: string
          deleted_at?: string | null
          deleted_by?: string | null
          id?: string
          last_edited_by?: string | null
          status?: string
          template_id?: string | null
          title?: string
          updated_at?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_sessions_template_id_fkey"
            columns: ["template_id"]
            isOneToOne: false
            referencedRelation: "v2_templates"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_collection_sessions_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_template_write_tokens: {
        Row: {
          actor_id: string
          template_id: string
          transaction_id: number
        }
        Insert: {
          actor_id: string
          template_id: string
          transaction_id: number
        }
        Update: {
          actor_id?: string
          template_id?: string
          transaction_id?: number
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_template_write_tokens_template_id_fkey"
            columns: ["template_id"]
            isOneToOne: false
            referencedRelation: "v2_templates"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_user_state: {
        Row: {
          baseline_version: number
          first_opened_at: string
          session_id: string
          user_id: string
        }
        Insert: {
          baseline_version?: number
          first_opened_at?: string
          session_id: string
          user_id: string
        }
        Update: {
          baseline_version?: number
          first_opened_at?: string
          session_id?: string
          user_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_user_state_session_id_fkey"
            columns: ["session_id"]
            isOneToOne: false
            referencedRelation: "v2_collection_sessions"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_collection_values: {
        Row: {
          created_at: string
          field_id: string
          id: string
          session_id: string
          updated_at: string
          updated_by: string | null
          updated_origin: string
          value: Json
        }
        Insert: {
          created_at?: string
          field_id: string
          id?: string
          session_id: string
          updated_at?: string
          updated_by?: string | null
          updated_origin?: string
          value?: Json
        }
        Update: {
          created_at?: string
          field_id?: string
          id?: string
          session_id?: string
          updated_at?: string
          updated_by?: string | null
          updated_origin?: string
          value?: Json
        }
        Relationships: [
          {
            foreignKeyName: "v2_collection_values_field_id_fkey"
            columns: ["field_id"]
            isOneToOne: false
            referencedRelation: "v2_template_fields"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_collection_values_session_id_fkey"
            columns: ["session_id"]
            isOneToOne: false
            referencedRelation: "v2_collection_sessions"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_currency_options: {
        Row: {
          active: boolean
          position: number
          symbol: string
          workspace_id: string
        }
        Insert: {
          active?: boolean
          position: number
          symbol: string
          workspace_id: string
        }
        Update: {
          active?: boolean
          position?: number
          symbol?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_currency_options_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_economic_calendar_imports: {
        Row: {
          event_id: string | null
          last_seen_at: string
          source_description: string
          source_event_at: string
          source_event_id: string
          source_key: string
          source_name: string
          workspace_id: string
        }
        Insert: {
          event_id?: string | null
          last_seen_at?: string
          source_description?: string
          source_event_at: string
          source_event_id: string
          source_key: string
          source_name: string
          workspace_id: string
        }
        Update: {
          event_id?: string | null
          last_seen_at?: string
          source_description?: string
          source_event_at?: string
          source_event_id?: string
          source_key?: string
          source_name?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_economic_calendar_imports_event_id_fkey"
            columns: ["event_id"]
            isOneToOne: false
            referencedRelation: "economic_events"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_economic_calendar_imports_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_economic_calendar_notification_settings: {
        Row: {
          important_offsets: number[]
          ordinary_offsets: number[]
          updated_at: string
          user_id: string
          workspace_id: string
        }
        Insert: {
          important_offsets?: number[]
          ordinary_offsets?: number[]
          updated_at?: string
          user_id: string
          workspace_id: string
        }
        Update: {
          important_offsets?: number[]
          ordinary_offsets?: number[]
          updated_at?: string
          user_id?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_economic_calendar_notification_settings_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_economic_event_type_reminder_preferences: {
        Row: {
          reminder_offsets: number[]
          type_key: string
          updated_at: string
          user_id: string
          workspace_id: string
        }
        Insert: {
          reminder_offsets: number[]
          type_key: string
          updated_at?: string
          user_id: string
          workspace_id: string
        }
        Update: {
          reminder_offsets?: number[]
          type_key?: string
          updated_at?: string
          user_id?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_economic_event_type_reminder_pref_workspace_id_type_key_fkey"
            columns: ["workspace_id", "type_key"]
            isOneToOne: false
            referencedRelation: "v2_economic_event_types"
            referencedColumns: ["workspace_id", "type_key"]
          },
        ]
      }
      v2_economic_event_types: {
        Row: {
          is_important: boolean
          label: string
          type_key: string
          updated_at: string
          workspace_id: string
        }
        Insert: {
          is_important?: boolean
          label: string
          type_key: string
          updated_at?: string
          workspace_id: string
        }
        Update: {
          is_important?: boolean
          label?: string
          type_key?: string
          updated_at?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_economic_event_types_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_economic_reminder_preferences: {
        Row: {
          event_id: string | null
          id: string
          preference_origin: string
          reminder_offsets: number[]
          schedule_id: string | null
          updated_at: string
          user_id: string
        }
        Insert: {
          event_id?: string | null
          id?: string
          preference_origin?: string
          reminder_offsets?: number[]
          schedule_id?: string | null
          updated_at?: string
          user_id?: string
        }
        Update: {
          event_id?: string | null
          id?: string
          preference_origin?: string
          reminder_offsets?: number[]
          schedule_id?: string | null
          updated_at?: string
          user_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_economic_reminder_preferences_event_id_fkey"
            columns: ["event_id"]
            isOneToOne: false
            referencedRelation: "economic_events"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_economic_reminder_preferences_schedule_id_fkey"
            columns: ["schedule_id"]
            isOneToOne: false
            referencedRelation: "v2_economic_schedules"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_economic_reminder_snoozes: {
        Row: {
          created_at: string
          original_due_at: string
          reminder_id: string
          snoozed_until: string
          updated_at: string
          user_id: string
        }
        Insert: {
          created_at?: string
          original_due_at: string
          reminder_id: string
          snoozed_until: string
          updated_at?: string
          user_id?: string
        }
        Update: {
          created_at?: string
          original_due_at?: string
          reminder_id?: string
          snoozed_until?: string
          updated_at?: string
          user_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_economic_reminder_snoozes_reminder_id_fkey"
            columns: ["reminder_id"]
            isOneToOne: false
            referencedRelation: "economic_event_reminders"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_economic_schedules: {
        Row: {
          active: boolean
          created_at: string
          created_by: string | null
          ends_on: string | null
          id: string
          importance: number
          market_calendar: string | null
          name: string
          reminder_offsets: number[]
          starts_on: string
          times: string[]
          timezone: string
          updated_at: string
          updated_by: string | null
          weekdays: number[]
          workspace_id: string
        }
        Insert: {
          active?: boolean
          created_at?: string
          created_by?: string | null
          ends_on?: string | null
          id?: string
          importance?: number
          market_calendar?: string | null
          name: string
          reminder_offsets?: number[]
          starts_on?: string
          times?: string[]
          timezone?: string
          updated_at?: string
          updated_by?: string | null
          weekdays?: number[]
          workspace_id: string
        }
        Update: {
          active?: boolean
          created_at?: string
          created_by?: string | null
          ends_on?: string | null
          id?: string
          importance?: number
          market_calendar?: string | null
          name?: string
          reminder_offsets?: number[]
          starts_on?: string
          times?: string[]
          timezone?: string
          updated_at?: string
          updated_by?: string | null
          weekdays?: number[]
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_economic_schedules_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_formulas: {
        Row: {
          created_at: string
          created_by: string | null
          id: string
          name: string
          normalized_name: string | null
          updated_at: string
          workspace_id: string
        }
        Insert: {
          created_at?: string
          created_by?: string | null
          id?: string
          name: string
          normalized_name?: string | null
          updated_at?: string
          workspace_id: string
        }
        Update: {
          created_at?: string
          created_by?: string | null
          id?: string
          name?: string
          normalized_name?: string | null
          updated_at?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_formulas_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_liquidity_ai_runs: {
        Row: {
          analysis: Json
          captured_at: string
          created_at: string
          created_by: string
          field_id: string
          id: string
          model: string
          observed_price: number | null
          previous_value: Json
          provider: string
          screenshot: string | null
          session_id: string
          source_url: string
          timeframe: string
          usage: Json | null
          workspace_id: string
        }
        Insert: {
          analysis?: Json
          captured_at: string
          created_at?: string
          created_by?: string
          field_id: string
          id?: string
          model: string
          observed_price?: number | null
          previous_value?: Json
          provider: string
          screenshot?: string | null
          session_id: string
          source_url: string
          timeframe: string
          usage?: Json | null
          workspace_id: string
        }
        Update: {
          analysis?: Json
          captured_at?: string
          created_at?: string
          created_by?: string
          field_id?: string
          id?: string
          model?: string
          observed_price?: number | null
          previous_value?: Json
          provider?: string
          screenshot?: string | null
          session_id?: string
          source_url?: string
          timeframe?: string
          usage?: Json | null
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_liquidity_ai_runs_session_id_fkey"
            columns: ["session_id"]
            isOneToOne: false
            referencedRelation: "v2_collection_sessions"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_liquidity_ai_runs_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_manual_batches: {
        Row: {
          author_id: string
          batch_id: string
          created_at: string
          fingerprint: string
          occurred_at: string
          server_fingerprint: string | null
          workspace_id: string
        }
        Insert: {
          author_id: string
          batch_id: string
          created_at?: string
          fingerprint: string
          occurred_at: string
          server_fingerprint?: string | null
          workspace_id: string
        }
        Update: {
          author_id?: string
          batch_id?: string
          created_at?: string
          fingerprint?: string
          occurred_at?: string
          server_fingerprint?: string | null
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_manual_batches_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_manual_entries: {
        Row: {
          archived_at: string | null
          archived_by: string | null
          batch_id: string
          break_even: boolean
          conclusions: string
          created_at: string
          created_by: string
          formula_name: string | null
          id: string
          notes: string
          occurred_at: string
          pnl: number | null
          row_key: string
          source_kind: string
          symbol: string
          updated_at: string
          updated_by: string | null
          workspace_id: string
        }
        Insert: {
          archived_at?: string | null
          archived_by?: string | null
          batch_id: string
          break_even?: boolean
          conclusions?: string
          created_at?: string
          created_by?: string
          formula_name?: string | null
          id?: string
          notes?: string
          occurred_at?: string
          pnl?: number | null
          row_key: string
          source_kind?: string
          symbol?: string
          updated_at?: string
          updated_by?: string | null
          workspace_id: string
        }
        Update: {
          archived_at?: string | null
          archived_by?: string | null
          batch_id?: string
          break_even?: boolean
          conclusions?: string
          created_at?: string
          created_by?: string
          formula_name?: string | null
          id?: string
          notes?: string
          occurred_at?: string
          pnl?: number | null
          row_key?: string
          source_kind?: string
          symbol?: string
          updated_at?: string
          updated_by?: string | null
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_manual_entries_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_news_items: {
        Row: {
          body: string
          category: string | null
          collection_session_id: string | null
          created_at: string
          created_by: string
          field_id: string | null
          id: string
          importance: string
          occurred_at: string
          rating_pending: boolean
          source_id: string | null
          source_name: string | null
          source_url: string | null
          updated_at: string
          updated_by: string | null
          updated_origin: string
          workspace_id: string
        }
        Insert: {
          body: string
          category?: string | null
          collection_session_id?: string | null
          created_at?: string
          created_by: string
          field_id?: string | null
          id?: string
          importance?: string
          occurred_at?: string
          rating_pending?: boolean
          source_id?: string | null
          source_name?: string | null
          source_url?: string | null
          updated_at?: string
          updated_by?: string | null
          updated_origin?: string
          workspace_id: string
        }
        Update: {
          body?: string
          category?: string | null
          collection_session_id?: string | null
          created_at?: string
          created_by?: string
          field_id?: string | null
          id?: string
          importance?: string
          occurred_at?: string
          rating_pending?: boolean
          source_id?: string | null
          source_name?: string | null
          source_url?: string | null
          updated_at?: string
          updated_by?: string | null
          updated_origin?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_news_items_collection_session_id_fkey"
            columns: ["collection_session_id"]
            isOneToOne: false
            referencedRelation: "v2_collection_sessions"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_news_items_field_id_fkey"
            columns: ["field_id"]
            isOneToOne: false
            referencedRelation: "v2_template_fields"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_news_items_source_id_fkey"
            columns: ["source_id"]
            isOneToOne: false
            referencedRelation: "v2_news_sources"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_news_items_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_news_pins: {
        Row: {
          field_id: string
          item_key: string
          logical_key: string
          origin_item_id: string
          origin_session_id: string
          pinned_at: string
          pinned_by: string | null
          snapshot: Json
          workspace_id: string
        }
        Insert: {
          field_id: string
          item_key: string
          logical_key: string
          origin_item_id: string
          origin_session_id: string
          pinned_at?: string
          pinned_by?: string | null
          snapshot: Json
          workspace_id: string
        }
        Update: {
          field_id?: string
          item_key?: string
          logical_key?: string
          origin_item_id?: string
          origin_session_id?: string
          pinned_at?: string
          pinned_by?: string | null
          snapshot?: Json
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_news_pins_field_id_fkey"
            columns: ["field_id"]
            isOneToOne: false
            referencedRelation: "v2_template_fields"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_news_pins_origin_session_id_fkey"
            columns: ["origin_session_id"]
            isOneToOne: false
            referencedRelation: "v2_collection_sessions"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_news_pins_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_news_sources: {
        Row: {
          created_at: string
          current_scenario: string | null
          current_sentiment: string
          id: string
          name: string
          status_defined: boolean
          updated_at: string
          updated_by: string | null
          workspace_id: string
        }
        Insert: {
          created_at?: string
          current_scenario?: string | null
          current_sentiment?: string
          id?: string
          name: string
          status_defined?: boolean
          updated_at?: string
          updated_by?: string | null
          workspace_id: string
        }
        Update: {
          created_at?: string
          current_scenario?: string | null
          current_sentiment?: string
          id?: string
          name?: string
          status_defined?: boolean
          updated_at?: string
          updated_by?: string | null
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_news_sources_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_news_status_sources: {
        Row: {
          created_at: string
          current_scenario: string | null
          current_sentiment: string
          id: string
          name: string
          status_defined: boolean
          status_only: boolean
          updated_at: string
          updated_by: string | null
          workspace_id: string
        }
        Insert: {
          created_at?: string
          current_scenario?: string | null
          current_sentiment?: string
          id?: string
          name: string
          status_defined?: boolean
          status_only?: boolean
          updated_at?: string
          updated_by?: string | null
          workspace_id: string
        }
        Update: {
          created_at?: string
          current_scenario?: string | null
          current_sentiment?: string
          id?: string
          name?: string
          status_defined?: boolean
          status_only?: boolean
          updated_at?: string
          updated_by?: string | null
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_news_status_sources_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_news_summary_hidden: {
        Row: {
          hidden_at: string
          item_id: string
          user_id: string
          workspace_id: string
        }
        Insert: {
          hidden_at?: string
          item_id: string
          user_id?: string
          workspace_id: string
        }
        Update: {
          hidden_at?: string
          item_id?: string
          user_id?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_news_summary_hidden_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_news_summary_preferences: {
        Row: {
          selected_source_ids: Json | null
          source_lists: Json
          user_id: string
          workspace_id: string
        }
        Insert: {
          selected_source_ids?: Json | null
          source_lists?: Json
          user_id?: string
          workspace_id: string
        }
        Update: {
          selected_source_ids?: Json | null
          source_lists?: Json
          user_id?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_news_summary_preferences_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_report_archives: {
        Row: {
          archived_at: string
          archived_by: string | null
          created_at: string
          id: string
          source_key: string
          source_kind: string
          updated_at: string
          workspace_id: string
        }
        Insert: {
          archived_at?: string
          archived_by?: string | null
          created_at?: string
          id?: string
          source_key: string
          source_kind: string
          updated_at?: string
          workspace_id: string
        }
        Update: {
          archived_at?: string
          archived_by?: string | null
          created_at?: string
          id?: string
          source_key?: string
          source_kind?: string
          updated_at?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_report_archives_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_report_settings: {
        Row: {
          be_max: number
          be_min: number
          created_at: string
          next_entry_at: string | null
          stats_mode: string
          updated_at: string
          user_id: string
          workspace_id: string
        }
        Insert: {
          be_max?: number
          be_min?: number
          created_at?: string
          next_entry_at?: string | null
          stats_mode?: string
          updated_at?: string
          user_id: string
          workspace_id: string
        }
        Update: {
          be_max?: number
          be_min?: number
          created_at?: string
          next_entry_at?: string | null
          stats_mode?: string
          updated_at?: string
          user_id?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_report_settings_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_scenario_alternatives: {
        Row: {
          content_html: string | null
          created_at: string
          id: string
          label: string
          position: number
          scenario_id: string
          updated_at: string
        }
        Insert: {
          content_html?: string | null
          created_at?: string
          id?: string
          label: string
          position?: number
          scenario_id: string
          updated_at?: string
        }
        Update: {
          content_html?: string | null
          created_at?: string
          id?: string
          label?: string
          position?: number
          scenario_id?: string
          updated_at?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_scenario_alternatives_scenario_id_fkey"
            columns: ["scenario_id"]
            isOneToOne: false
            referencedRelation: "v2_scenarios"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_scenario_values: {
        Row: {
          created_at: string
          field_id: string
          id: string
          scenario_id: string
          updated_at: string
          updated_by: string | null
          value: Json
        }
        Insert: {
          created_at?: string
          field_id: string
          id?: string
          scenario_id: string
          updated_at?: string
          updated_by?: string | null
          value?: Json
        }
        Update: {
          created_at?: string
          field_id?: string
          id?: string
          scenario_id?: string
          updated_at?: string
          updated_by?: string | null
          value?: Json
        }
        Relationships: [
          {
            foreignKeyName: "v2_scenario_values_field_id_fkey"
            columns: ["field_id"]
            isOneToOne: false
            referencedRelation: "v2_template_fields"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_scenario_values_scenario_id_fkey"
            columns: ["scenario_id"]
            isOneToOne: false
            referencedRelation: "v2_scenarios"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_scenarios: {
        Row: {
          alternatives: Json
          asset_symbol: string
          author_name: string | null
          created_at: string
          created_by: string
          extra_boxes: Json
          general_evidence: Json
          id: string
          invalidating_evidence: Json
          invalidation_html: string | null
          pinned: boolean
          position: number
          reasons_html: string | null
          scenario_html: string | null
          status: string
          supporting_evidence: Json
          template_id: string | null
          title: string
          updated_at: string
          updated_by: string | null
          workspace_id: string
        }
        Insert: {
          alternatives?: Json
          asset_symbol?: string
          author_name?: string | null
          created_at?: string
          created_by: string
          extra_boxes?: Json
          general_evidence?: Json
          id?: string
          invalidating_evidence?: Json
          invalidation_html?: string | null
          pinned?: boolean
          position?: number
          reasons_html?: string | null
          scenario_html?: string | null
          status?: string
          supporting_evidence?: Json
          template_id?: string | null
          title: string
          updated_at?: string
          updated_by?: string | null
          workspace_id: string
        }
        Update: {
          alternatives?: Json
          asset_symbol?: string
          author_name?: string | null
          created_at?: string
          created_by?: string
          extra_boxes?: Json
          general_evidence?: Json
          id?: string
          invalidating_evidence?: Json
          invalidation_html?: string | null
          pinned?: boolean
          position?: number
          reasons_html?: string | null
          scenario_html?: string | null
          status?: string
          supporting_evidence?: Json
          template_id?: string | null
          title?: string
          updated_at?: string
          updated_by?: string | null
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_scenarios_template_id_fkey"
            columns: ["template_id"]
            isOneToOne: false
            referencedRelation: "v2_templates"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_scenarios_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_spot_assets: {
        Row: {
          category: string
          created_at: string
          created_by: string
          device_name: string | null
          id: string
          location_name: string | null
          location_type: string | null
          opening_unit_cost: number | null
          quantity: number
          security_notes: string | null
          symbol: string
          template_id: string | null
          updated_at: string
          updated_by: string | null
          wallet_address: string | null
          workspace_id: string
        }
        Insert: {
          category: string
          created_at?: string
          created_by: string
          device_name?: string | null
          id?: string
          location_name?: string | null
          location_type?: string | null
          opening_unit_cost?: number | null
          quantity?: number
          security_notes?: string | null
          symbol: string
          template_id?: string | null
          updated_at?: string
          updated_by?: string | null
          wallet_address?: string | null
          workspace_id: string
        }
        Update: {
          category?: string
          created_at?: string
          created_by?: string
          device_name?: string | null
          id?: string
          location_name?: string | null
          location_type?: string | null
          opening_unit_cost?: number | null
          quantity?: number
          security_notes?: string | null
          symbol?: string
          template_id?: string | null
          updated_at?: string
          updated_by?: string | null
          wallet_address?: string | null
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_spot_assets_template_id_fkey"
            columns: ["template_id"]
            isOneToOne: false
            referencedRelation: "v2_templates"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_spot_assets_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_spot_orders: {
        Row: {
          asset_id: string
          created_at: string
          created_by: string
          id: string
          order_type: string | null
          price: number
          quantity: number
          side: string
          status: string
          updated_at: string
          updated_by: string | null
          venue: string | null
          workspace_id: string
        }
        Insert: {
          asset_id: string
          created_at?: string
          created_by: string
          id?: string
          order_type?: string | null
          price: number
          quantity: number
          side: string
          status?: string
          updated_at?: string
          updated_by?: string | null
          venue?: string | null
          workspace_id: string
        }
        Update: {
          asset_id?: string
          created_at?: string
          created_by?: string
          id?: string
          order_type?: string | null
          price?: number
          quantity?: number
          side?: string
          status?: string
          updated_at?: string
          updated_by?: string | null
          venue?: string | null
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_spot_orders_asset_id_fkey"
            columns: ["asset_id"]
            isOneToOne: false
            referencedRelation: "v2_spot_assets"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_spot_orders_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_spot_target_acknowledgements: {
        Row: {
          acknowledged_at: string
          acknowledged_price: number | null
          target_id: string
          user_id: string
        }
        Insert: {
          acknowledged_at?: string
          acknowledged_price?: number | null
          target_id: string
          user_id?: string
        }
        Update: {
          acknowledged_at?: string
          acknowledged_price?: number | null
          target_id?: string
          user_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_spot_target_acknowledgements_target_id_fkey"
            columns: ["target_id"]
            isOneToOne: false
            referencedRelation: "v2_spot_targets"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_spot_targets: {
        Row: {
          acknowledged_by: Json
          active: boolean
          asset_id: string
          created_at: string
          created_by: string
          direction: string
          id: string
          target_price: number
          updated_at: string
          updated_by: string | null
          workspace_id: string
        }
        Insert: {
          acknowledged_by?: Json
          active?: boolean
          asset_id: string
          created_at?: string
          created_by: string
          direction?: string
          id?: string
          target_price: number
          updated_at?: string
          updated_by?: string | null
          workspace_id: string
        }
        Update: {
          acknowledged_by?: Json
          active?: boolean
          asset_id?: string
          created_at?: string
          created_by?: string
          direction?: string
          id?: string
          target_price?: number
          updated_at?: string
          updated_by?: string | null
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_spot_targets_asset_id_fkey"
            columns: ["asset_id"]
            isOneToOne: false
            referencedRelation: "v2_spot_assets"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_spot_targets_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_spot_transactions: {
        Row: {
          asset_id: string
          created_at: string
          created_by: string
          fee: number
          id: string
          lot_key: string | null
          note: string | null
          occurred_at: string
          price: number
          quantity: number
          side: string
          updated_at: string
          updated_by: string | null
          venue: string | null
          workspace_id: string
        }
        Insert: {
          asset_id: string
          created_at?: string
          created_by: string
          fee?: number
          id?: string
          lot_key?: string | null
          note?: string | null
          occurred_at?: string
          price: number
          quantity: number
          side: string
          updated_at?: string
          updated_by?: string | null
          venue?: string | null
          workspace_id: string
        }
        Update: {
          asset_id?: string
          created_at?: string
          created_by?: string
          fee?: number
          id?: string
          lot_key?: string | null
          note?: string | null
          occurred_at?: string
          price?: number
          quantity?: number
          side?: string
          updated_at?: string
          updated_by?: string | null
          venue?: string | null
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_spot_transactions_asset_id_fkey"
            columns: ["asset_id"]
            isOneToOne: false
            referencedRelation: "v2_spot_assets"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_spot_transactions_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_spot_values: {
        Row: {
          asset_id: string
          created_at: string
          field_id: string
          id: string
          updated_at: string
          updated_by: string | null
          value: Json
        }
        Insert: {
          asset_id: string
          created_at?: string
          field_id: string
          id?: string
          updated_at?: string
          updated_by?: string | null
          value?: Json
        }
        Update: {
          asset_id?: string
          created_at?: string
          field_id?: string
          id?: string
          updated_at?: string
          updated_by?: string | null
          value?: Json
        }
        Relationships: [
          {
            foreignKeyName: "v2_spot_values_asset_id_fkey"
            columns: ["asset_id"]
            isOneToOne: false
            referencedRelation: "v2_spot_assets"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_spot_values_field_id_fkey"
            columns: ["field_id"]
            isOneToOne: false
            referencedRelation: "v2_template_fields"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_template_fields: {
        Row: {
          active: boolean
          created_at: string
          default_align: string
          field_key: string
          field_type: string
          id: string
          pinned_scope: string
          position: number
          required: boolean
          section_id: string
          settings: Json
          title: string
          updated_at: string
        }
        Insert: {
          active?: boolean
          created_at?: string
          default_align?: string
          field_key: string
          field_type: string
          id?: string
          pinned_scope?: string
          position?: number
          required?: boolean
          section_id: string
          settings?: Json
          title: string
          updated_at?: string
        }
        Update: {
          active?: boolean
          created_at?: string
          default_align?: string
          field_key?: string
          field_type?: string
          id?: string
          pinned_scope?: string
          position?: number
          required?: boolean
          section_id?: string
          settings?: Json
          title?: string
          updated_at?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_template_fields_section_id_fkey"
            columns: ["section_id"]
            isOneToOne: false
            referencedRelation: "v2_template_sections"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_template_sections: {
        Row: {
          active: boolean
          created_at: string
          id: string
          position: number
          settings: Json
          template_id: string
          title: string
          updated_at: string
        }
        Insert: {
          active?: boolean
          created_at?: string
          id?: string
          position?: number
          settings?: Json
          template_id: string
          title: string
          updated_at?: string
        }
        Update: {
          active?: boolean
          created_at?: string
          id?: string
          position?: number
          settings?: Json
          template_id?: string
          title?: string
          updated_at?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_template_sections_template_id_fkey"
            columns: ["template_id"]
            isOneToOne: false
            referencedRelation: "v2_templates"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_templates: {
        Row: {
          active: boolean
          asset_symbol: string | null
          created_at: string
          created_by: string | null
          editor_revision: number
          id: string
          is_default: boolean
          kind: string
          name: string
          settings: Json
          updated_at: string
          workspace_id: string
        }
        Insert: {
          active?: boolean
          asset_symbol?: string | null
          created_at?: string
          created_by?: string | null
          editor_revision?: number
          id?: string
          is_default?: boolean
          kind: string
          name: string
          settings?: Json
          updated_at?: string
          workspace_id: string
        }
        Update: {
          active?: boolean
          asset_symbol?: string | null
          created_at?: string
          created_by?: string | null
          editor_revision?: number
          id?: string
          is_default?: boolean
          kind?: string
          name?: string
          settings?: Json
          updated_at?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_templates_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_trade_values: {
        Row: {
          created_at: string
          field_id: string
          id: string
          trade_id: string
          updated_at: string
          updated_by: string | null
          value: Json
        }
        Insert: {
          created_at?: string
          field_id: string
          id?: string
          trade_id: string
          updated_at?: string
          updated_by?: string | null
          value?: Json
        }
        Update: {
          created_at?: string
          field_id?: string
          id?: string
          trade_id?: string
          updated_at?: string
          updated_by?: string | null
          value?: Json
        }
        Relationships: [
          {
            foreignKeyName: "v2_trade_values_field_id_fkey"
            columns: ["field_id"]
            isOneToOne: false
            referencedRelation: "v2_template_fields"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_trade_values_trade_id_fkey"
            columns: ["trade_id"]
            isOneToOne: false
            referencedRelation: "v2_trades"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_trades: {
        Row: {
          activated_at: string | null
          allow_cross_asset_scenario: boolean
          archived_at: string | null
          asset_symbol: string
          bot_alert_html: string
          break_even: boolean
          closed_at: string | null
          created_at: string
          created_by: string
          cycle_number: number
          first_close_at: string | null
          first_pnl_at: string | null
          formula_name: string | null
          id: string
          pnl: number | null
          root_trade_id: string | null
          scenario_id: string | null
          source_kind: string
          status: string
          template_id: string | null
          title: string
          updated_at: string
          updated_by: string | null
          workspace_id: string
        }
        Insert: {
          activated_at?: string | null
          allow_cross_asset_scenario?: boolean
          archived_at?: string | null
          asset_symbol?: string
          bot_alert_html?: string
          break_even?: boolean
          closed_at?: string | null
          created_at?: string
          created_by: string
          cycle_number?: number
          first_close_at?: string | null
          first_pnl_at?: string | null
          formula_name?: string | null
          id?: string
          pnl?: number | null
          root_trade_id?: string | null
          scenario_id?: string | null
          source_kind?: string
          status?: string
          template_id?: string | null
          title: string
          updated_at?: string
          updated_by?: string | null
          workspace_id: string
        }
        Update: {
          activated_at?: string | null
          allow_cross_asset_scenario?: boolean
          archived_at?: string | null
          asset_symbol?: string
          bot_alert_html?: string
          break_even?: boolean
          closed_at?: string | null
          created_at?: string
          created_by?: string
          cycle_number?: number
          first_close_at?: string | null
          first_pnl_at?: string | null
          formula_name?: string | null
          id?: string
          pnl?: number | null
          root_trade_id?: string | null
          scenario_id?: string | null
          source_kind?: string
          status?: string
          template_id?: string | null
          title?: string
          updated_at?: string
          updated_by?: string | null
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_trades_root_trade_id_fkey"
            columns: ["root_trade_id"]
            isOneToOne: false
            referencedRelation: "v2_trades"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_trades_scenario_id_fkey"
            columns: ["scenario_id"]
            isOneToOne: false
            referencedRelation: "v2_scenarios"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_trades_template_id_fkey"
            columns: ["template_id"]
            isOneToOne: false
            referencedRelation: "v2_templates"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_trades_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_us_equity_calendar_coverage: {
        Row: {
          singleton: boolean
          source_url: string
          verified_from: string
          verified_on: string
          verified_through: string
        }
        Insert: {
          singleton?: boolean
          source_url: string
          verified_from: string
          verified_on: string
          verified_through: string
        }
        Update: {
          singleton?: boolean
          source_url?: string
          verified_from?: string
          verified_on?: string
          verified_through?: string
        }
        Relationships: []
      }
      v2_us_equity_calendar_exceptions: {
        Row: {
          close_time: string | null
          closed: boolean
          market_date: string
          reason: string
        }
        Insert: {
          close_time?: string | null
          closed: boolean
          market_date: string
          reason: string
        }
        Update: {
          close_time?: string | null
          closed?: boolean
          market_date?: string
          reason?: string
        }
        Relationships: []
      }
      v2_us_equity_open_occurrences: {
        Row: {
          event_id: string | null
          market_date: string
          schedule_id: string
          source_baseline: Json
        }
        Insert: {
          event_id?: string | null
          market_date: string
          schedule_id: string
          source_baseline: Json
        }
        Update: {
          event_id?: string | null
          market_date?: string
          schedule_id?: string
          source_baseline?: Json
        }
        Relationships: [
          {
            foreignKeyName: "v2_us_equity_open_occurrences_event_id_fkey"
            columns: ["event_id"]
            isOneToOne: false
            referencedRelation: "economic_events"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "v2_us_equity_open_occurrences_schedule_id_fkey"
            columns: ["schedule_id"]
            isOneToOne: false
            referencedRelation: "v2_economic_schedules"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_workspace_members: {
        Row: {
          created_at: string
          role: string
          user_id: string
          workspace_id: string
        }
        Insert: {
          created_at?: string
          role?: string
          user_id: string
          workspace_id: string
        }
        Update: {
          created_at?: string
          role?: string
          user_id?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_workspace_members_workspace_id_fkey"
            columns: ["workspace_id"]
            isOneToOne: false
            referencedRelation: "v2_workspaces"
            referencedColumns: ["id"]
          },
        ]
      }
      v2_workspace_preferences: {
        Row: {
          updated_at: string
          user_id: string
          workspace_id: string
        }
        Insert: {
          updated_at?: string
          user_id: string
          workspace_id: string
        }
        Update: {
          updated_at?: string
          user_id?: string
          workspace_id?: string
        }
        Relationships: [
          {
            foreignKeyName: "v2_workspace_preferences_member_fkey"
            columns: ["workspace_id", "user_id"]
            isOneToOne: false
            referencedRelation: "v2_workspace_members"
            referencedColumns: ["workspace_id", "user_id"]
          },
        ]
      }
      v2_workspaces: {
        Row: {
          created_at: string
          id: string
          name: string
          owner_id: string
          updated_at: string
        }
        Insert: {
          created_at?: string
          id?: string
          name?: string
          owner_id: string
          updated_at?: string
        }
        Update: {
          created_at?: string
          id?: string
          name?: string
          owner_id?: string
          updated_at?: string
        }
        Relationships: []
      }
    }
    Views: {
      news_summary_rows: {
        Row: {
          body: string | null
          created_at: string | null
          editor_name: string | null
          group_name: string | null
          id: string | null
          importance: string | null
          importance_rank: number | null
          module_type: string | null
          position: number | null
          project_created_at: string | null
          project_id: string | null
          project_name: string | null
          question_id: string | null
          source_name: string | null
          stage_name: string | null
          updated_at: string | null
          updated_by: string | null
        }
        Relationships: [
          {
            foreignKeyName: "project_news_rows_project_id_fkey"
            columns: ["project_id"]
            isOneToOne: false
            referencedRelation: "projects"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "project_news_rows_question_id_fkey"
            columns: ["question_id"]
            isOneToOne: false
            referencedRelation: "template_questions"
            referencedColumns: ["id"]
          },
        ]
      }
    }
    Functions: {
      add_project_member_by_email: {
        Args: { _email: string; _project_id: string; _role: string }
        Returns: Json
      }
      clone_version_to_template: {
        Args: { _source_version: string; _target_template: string }
        Returns: {
          id: string
        }[]
      }
      copy_template_group_atomic: {
        Args: {
          _keep_history_key?: boolean
          _source_group_id: string
          _target_stage_id: string
          _title: string
        }
        Returns: string
      }
      create_trade_cycle: {
        Args: { _project_id: string }
        Returns: {
          created_at: string
          created_by: string | null
          cycle_number: number
          first_close_at: string | null
          first_pnl_at: string | null
          formula_name: string | null
          id: string
          project_id: string
        }[]
        SetofOptions: {
          from: "*"
          to: "project_trade_cycles"
          isOneToOne: false
          isSetofReturn: true
        }
      }
      duplicate_legacy_project: {
        Args: { _project_id: string }
        Returns: string
      }
      duplicate_legacy_template_version: {
        Args: { p_version_id: string }
        Returns: {
          created_at: string
          id: string
          published_at: string | null
          status: string
          template_id: string
          version_number: number
        }[]
        SetofOptions: {
          from: "*"
          to: "template_versions"
          isOneToOne: false
          isSetofReturn: true
        }
      }
      execute_v2_trade_button: {
        Args: { p_field_id: string; p_trade_id: string }
        Returns: {
          activated_at: string | null
          allow_cross_asset_scenario: boolean
          archived_at: string | null
          asset_symbol: string
          bot_alert_html: string
          break_even: boolean
          closed_at: string | null
          created_at: string
          created_by: string
          cycle_number: number
          first_close_at: string | null
          first_pnl_at: string | null
          formula_name: string | null
          id: string
          pnl: number | null
          root_trade_id: string | null
          scenario_id: string | null
          source_kind: string
          status: string
          template_id: string | null
          title: string
          updated_at: string
          updated_by: string | null
          workspace_id: string
        }
        SetofOptions: {
          from: "*"
          to: "v2_trades"
          isOneToOne: true
          isSetofReturn: false
        }
      }
      import_template_tree_atomic: {
        Args: {
          _stages?: Json
          _target_group_id?: string
          _target_stage_id?: string
          _target_version_id: string
        }
        Returns: string
      }
      inline_paths_referenced: {
        Args: { _paths: string[]; _project_id: string }
        Returns: string[]
      }
      my_unread_questions: {
        Args: { _project_id?: string }
        Returns: {
          actor_id: string
          group_id: string
          kind: string
          latest_at: string
          project_id: string
          question_id: string
          question_key: string
          stage_id: string
          trade_cycle_id: string
        }[]
      }
      new_v2_trade_cycle: {
        Args: { p_trade_id: string }
        Returns: {
          activated_at: string | null
          allow_cross_asset_scenario: boolean
          archived_at: string | null
          asset_symbol: string
          bot_alert_html: string
          break_even: boolean
          closed_at: string | null
          created_at: string
          created_by: string
          cycle_number: number
          first_close_at: string | null
          first_pnl_at: string | null
          formula_name: string | null
          id: string
          pnl: number | null
          root_trade_id: string | null
          scenario_id: string | null
          source_kind: string
          status: string
          template_id: string | null
          title: string
          updated_at: string
          updated_by: string | null
          workspace_id: string
        }
        SetofOptions: {
          from: "*"
          to: "v2_trades"
          isOneToOne: true
          isSetofReturn: false
        }
      }
      open_project:
        | { Args: { _project_id: string; _stage_id?: string }; Returns: string }
        | {
            Args: {
              _project_id: string
              _stage_id: string
              _trade_cycle_id: string
            }
            Returns: string
          }
      patch_v2_entity_rows: {
        Args: {
          p_entity_id: string
          p_field_id: string
          p_operation: string
          p_parent_row_id?: string
          p_patch?: Json
          p_row_id: string
          p_scope: string
        }
        Returns: Json
      }
      prepare_v2_worksheet: {
        Args: { p_entity_id: string; p_kind: string }
        Returns: string
      }
      project_latest_activity: {
        Args: { _project_id: string }
        Returns: {
          actor_id: string
          kind: string
          latest_at: string
          question_id: string
          trade_cycle_id: string
        }[]
      }
      reorder_v2_scenarios: {
        Args: { p_ids: string[]; p_workspace_id: string }
        Returns: undefined
      }
      save_v2_scenario: {
        Args: { p_scenario: Json; p_values?: Json }
        Returns: {
          alternatives: Json
          asset_symbol: string
          author_name: string | null
          created_at: string
          created_by: string
          extra_boxes: Json
          general_evidence: Json
          id: string
          invalidating_evidence: Json
          invalidation_html: string | null
          pinned: boolean
          position: number
          reasons_html: string | null
          scenario_html: string | null
          status: string
          supporting_evidence: Json
          template_id: string | null
          title: string
          updated_at: string
          updated_by: string | null
          workspace_id: string
        }
        SetofOptions: {
          from: "*"
          to: "v2_scenarios"
          isOneToOne: true
          isSetofReturn: false
        }
      }
      save_v2_trade_value: {
        Args: { p_field_id: string; p_trade_id: string; p_value: Json }
        Returns: undefined
      }
      save_v2_trade_value_receipt: {
        Args: { p_field_id: string; p_trade_id: string; p_value: Json }
        Returns: {
          created_at: string
          field_id: string
          id: string
          trade_id: string
          updated_at: string
          updated_by: string | null
          value: Json
        }[]
        SetofOptions: {
          from: "*"
          to: "v2_trade_values"
          isOneToOne: false
          isSetofReturn: true
        }
      }
      sync_my_profile: { Args: never; Returns: undefined }
      v2_ack_collection_field: {
        Args: {
          p_field_id: string
          p_observed_version: number
          p_session_id: string
        }
        Returns: undefined
      }
      v2_ack_collection_news_row: {
        Args: {
          p_field_id: string
          p_observed_version: number
          p_row_id: string
          p_session_id: string
        }
        Returns: undefined
      }
      v2_acknowledge_economic_reminder: {
        Args: { p_reminder_id: string }
        Returns: undefined
      }
      v2_acknowledge_economic_reminders: {
        Args: { p_reminder_ids: string[] }
        Returns: undefined
      }
      v2_acquire_ai_lease: {
        Args: {
          p_hourly_limit?: number
          p_scope_key: string
          p_ttl_seconds?: number
          p_workspace_id: string
        }
        Returns: Json
      }
      v2_add_collection_previous_news_evidence: {
        Args: {
          p_expected_snapshot: Json
          p_field_id: string
          p_image_copies?: Json
          p_pin_id?: string
          p_role: string
          p_scenario_id: string
          p_selector: Json
          p_session_id: string
          p_source_field_id: string
          p_source_session_id: string
        }
        Returns: Json
      }
      v2_add_collection_scenario_evidence: {
        Args: {
          p_expected_snapshot: Json
          p_field_id: string
          p_image_copies?: Json
          p_news_id: string
          p_role: string
          p_scenario_id: string
          p_session_id: string
        }
        Returns: Json
      }
      v2_add_currency_option: {
        Args: { p_symbol: string; p_workspace_id: string }
        Returns: string
      }
      v2_add_formula: {
        Args: { p_name: string; p_workspace_id: string }
        Returns: string
      }
      v2_ai_liquidity_levels: {
        Args: { p_levels: Json; p_row_id: string }
        Returns: Json
      }
      v2_archive_collection_news_feedback: {
        Args: {
          p_detail?: string
          p_field_id: string
          p_reason: string
          p_row_id: string
          p_session_id: string
          p_workspace_id: string
        }
        Returns: Json
      }
      v2_archive_trade: { Args: { p_trade_id: string }; Returns: undefined }
      v2_attach_collection_scenario: {
        Args: { p_scenario_id: string; p_session_id: string }
        Returns: Json
      }
      v2_calendar_can_edit: {
        Args: { p_workspace_id: string }
        Returns: boolean
      }
      v2_calendar_can_read: {
        Args: { p_workspace_id: string }
        Returns: boolean
      }
      v2_can_configure_reminders: {
        Args: { p_event_id: string; p_schedule_id: string }
        Returns: boolean
      }
      v2_can_edit_workspace: {
        Args: { p_workspace_id: string }
        Returns: boolean
      }
      v2_changed_collection_news_rows: {
        Args: { p_after: Json; p_before: Json }
        Returns: string[]
      }
      v2_claim_collection_news_source: {
        Args: { p_item: Json; p_run_id: string }
        Returns: boolean
      }
      v2_clear_collection_values: {
        Args: { p_field_id?: string; p_session_id: string; p_sheet_key: string }
        Returns: number
      }
      v2_collection_activity: {
        Args: { p_workspace_id: string }
        Returns: {
          is_new: boolean
          session_id: string
          unread: Json
        }[]
      }
      v2_collection_actor_label: { Args: { p_actor: string }; Returns: string }
      v2_collection_autofill_apply: {
        Args: {
          p_cursors: Json
          p_payload: Json
          p_run_id: string
          p_scan_started_at: string
        }
        Returns: Json
      }
      v2_collection_autofill_apply_calendar: {
        Args: {
          p_cursors: Json
          p_run_id: string
          p_scan_started_at: string
          p_snapshot: Json
        }
        Returns: Json
      }
      v2_collection_autofill_claim:
        | {
            Args: {
              p_field_id: string
              p_manual?: boolean
              p_session_id: string
            }
            Returns: Json
          }
        | {
            Args: {
              p_field_id: string
              p_manual: boolean
              p_run_id: string
              p_session_id: string
            }
            Returns: Json
          }
      v2_collection_autofill_due: {
        Args: { p_limit?: number }
        Returns: {
          field_id: string
          session_id: string
        }[]
      }
      v2_collection_autofill_fail: {
        Args: { p_message: string; p_run_id: string }
        Returns: undefined
      }
      v2_collection_autofill_known_items: {
        Args: { p_keys: string[]; p_run_id: string }
        Returns: string[]
      }
      v2_collection_autofill_publication_profile: {
        Args: { p_field_id: string }
        Returns: Json
      }
      v2_collection_autofill_record_publications: {
        Args: {
          p_coverage_from: string
          p_field_id: string
          p_items: Json
          p_run_id: string
          p_scanned_at: string
          p_source_url: string
        }
        Returns: Json
      }
      v2_collection_autofill_rollover: { Args: never; Returns: number }
      v2_collection_autofill_save_publication_model: {
        Args: { p_evidence_revision: string; p_field_id: string; p_model: Json }
        Returns: boolean
      }
      v2_collection_autofill_target: {
        Args: { p_template_id: string; p_workspace_id: string }
        Returns: string
      }
      v2_collection_current_values: {
        Args: { p_session_id: string }
        Returns: {
          created_at: string
          field_id: string
          id: string
          session_id: string
          updated_at: string
          updated_by: string | null
          updated_origin: string
          value: Json
        }[]
        SetofOptions: {
          from: "*"
          to: "v2_collection_values"
          isOneToOne: false
          isSetofReturn: true
        }
      }
      v2_collection_editor_snapshot: {
        Args: { p_template_id: string }
        Returns: Json
      }
      v2_collection_embedded_news_rows: {
        Args: { p_value: Json }
        Returns: Json
      }
      v2_collection_expiry_maintain: {
        Args: { p_limit?: number }
        Returns: number
      }
      v2_collection_expiry_market_claim: {
        Args: { p_limit?: number }
        Returns: Json
      }
      v2_collection_expiry_market_finish: {
        Args: { p_candle: Json; p_day: string; p_lease_id: string }
        Returns: boolean
      }
      v2_collection_expiry_statistics: {
        Args: { p_field_id: string; p_limit?: number; p_workspace_id: string }
        Returns: Json
      }
      v2_collection_field_contract: {
        Args: { p_settings: Json }
        Returns: Json
      }
      v2_collection_field_logical_key: {
        Args: { p_field: Json }
        Returns: string
      }
      v2_collection_historical_snapshot: {
        Args: { p_mode?: string; p_session_id?: string; p_workspace_id: string }
        Returns: {
          field: Json
          field_id: string
          logical_key: string
          news_items: Json
          section: Json
          session_created_at: string
          session_id: string
          sheet_key: string
          snapshot_key: string
          updated_at: string
          updated_by: string
          value: Json
        }[]
      }
      v2_collection_history: {
        Args: { p_trash?: boolean; p_workspace_id: string }
        Returns: {
          created_at: string
          created_by: string
          creation_origin: string
          creator_label: string
          deleted_at: string
          done: number
          id: string
          is_new: boolean
          last_edited_by: string
          percent: number
          status: string
          template_id: string
          title: string
          total: number
          unread: Json
          updated_at: string
          workspace_id: string
        }[]
      }
      v2_collection_item_count: {
        Args: { p_field_id: string; p_session_id: string }
        Returns: number
      }
      v2_collection_locked_live_value: {
        Args: { p_field_id: string; p_session_id: string }
        Returns: Json
      }
      v2_collection_managed_snapshot_merge: {
        Args: {
          p_current: Json
          p_incoming: Json
          p_run_id?: string
          p_workspace_id: string
        }
        Returns: Json
      }
      v2_collection_managed_snapshot_valid: {
        Args: { p_run_id?: string; p_value: Json; p_workspace_id: string }
        Returns: boolean
      }
      v2_collection_previous_pin_image_retained: {
        Args: { p_name: string }
        Returns: boolean
      }
      v2_collection_scenario_bundle: {
        Args: { p_scenario_id: string; p_session_id: string }
        Returns: Json
      }
      v2_collection_scenario_choices: {
        Args: {
          p_before_created_at?: string
          p_before_id?: string
          p_limit?: number
          p_session_id: string
        }
        Returns: Json
      }
      v2_collection_scenario_role_fields: {
        Args: { p_template_id: string }
        Returns: Json
      }
      v2_collection_scenarios: { Args: { p_session_id: string }; Returns: Json }
      v2_collection_sheet_key: { Args: { p_section: Json }; Returns: string }
      v2_collection_snapshot: {
        Args: { p_mode?: string; p_session_id?: string; p_workspace_id: string }
        Returns: {
          field: Json
          field_id: string
          logical_key: string
          news_items: Json
          section: Json
          session_created_at: string
          session_id: string
          sheet_key: string
          snapshot_key: string
          updated_at: string
          updated_by: string
          value: Json
        }[]
      }
      v2_collection_unbias_weekly_stored_valid: {
        Args: { p_run_id?: string; p_value: Json; p_workspace_id?: string }
        Returns: boolean
      }
      v2_collection_unbias_weekly_valid: {
        Args: { p_run_id?: string; p_value: Json; p_workspace_id: string }
        Returns: boolean
      }
      v2_collection_value_filled: {
        Args: { p_type: string; p_value: Json }
        Returns: boolean
      }
      v2_commit_liquidity_ai_run: {
        Args: {
          p_audit: Json
          p_baseline_updated_at: string
          p_field_id: string
          p_results: Json
          p_session_id: string
        }
        Returns: Json
      }
      v2_configured_trade_action_allowed: {
        Args: { p_action: string; p_trade_id: string }
        Returns: boolean
      }
      v2_create_collection_scenario: {
        Args: { p_request_id: string; p_session_id: string }
        Returns: Json
      }
      v2_create_empty_trade_scenario: {
        Args: {
          p_asset_symbol: string
          p_title: string
          p_workspace_id: string
        }
        Returns: string
      }
      v2_create_trade: {
        Args: { p_scenario?: Json; p_scenario_values?: Json; p_trade: Json }
        Returns: {
          activated_at: string | null
          allow_cross_asset_scenario: boolean
          archived_at: string | null
          asset_symbol: string
          bot_alert_html: string
          break_even: boolean
          closed_at: string | null
          created_at: string
          created_by: string
          cycle_number: number
          first_close_at: string | null
          first_pnl_at: string | null
          formula_name: string | null
          id: string
          pnl: number | null
          root_trade_id: string | null
          scenario_id: string | null
          source_kind: string
          status: string
          template_id: string | null
          title: string
          updated_at: string
          updated_by: string | null
          workspace_id: string
        }
        SetofOptions: {
          from: "*"
          to: "v2_trades"
          isOneToOne: true
          isSetofReturn: false
        }
      }
      v2_create_us_equity_open_schedule: {
        Args: { p_workspace_id: string }
        Returns: {
          active: boolean
          created_at: string
          created_by: string | null
          ends_on: string | null
          id: string
          importance: number
          market_calendar: string | null
          name: string
          reminder_offsets: number[]
          starts_on: string
          times: string[]
          timezone: string
          updated_at: string
          updated_by: string | null
          weekdays: number[]
          workspace_id: string
        }
        SetofOptions: {
          from: "*"
          to: "v2_economic_schedules"
          isOneToOne: true
          isSetofReturn: false
        }
      }
      v2_economic_reminder_state: { Args: never; Returns: Json }
      v2_editable_legacy_cycles: {
        Args: { p_cycle_ids: string[] }
        Returns: {
          cycle_id: string
        }[]
      }
      v2_empty_generated_worksheet: {
        Args: { p_entity_id: string; p_kind: string; p_template_id: string }
        Returns: boolean
      }
      v2_ensure_my_workspace: { Args: never; Returns: string }
      v2_expiry_decimal: {
        Args: { p_price?: boolean; p_value: string }
        Returns: number
      }
      v2_expiry_record_value: {
        Args: {
          p_logical_key: string
          p_updated_at: string
          p_updated_origin: string
          p_value: Json
          p_workspace_id: string
        }
        Returns: undefined
      }
      v2_expiry_valid_date: { Args: { p_value: string }; Returns: string }
      v2_field_complete: {
        Args: { p_type: string; p_value: Json }
        Returns: boolean
      }
      v2_has_personal_reminder_choice: {
        Args: { p_event_id: string }
        Returns: boolean
      }
      v2_hide_news_summary_item: {
        Args: { p_item_id: string; p_workspace_id: string }
        Returns: undefined
      }
      v2_image_workspace: { Args: { p_name: string }; Returns: string }
      v2_import_complete_legacy_template: {
        Args: { p_kind: string; p_workspace_id: string }
        Returns: string
      }
      v2_import_legacy_collection_template: {
        Args: { p_workspace_id: string }
        Returns: string
      }
      v2_import_legacy_trade_template: {
        Args: { p_workspace_id: string }
        Returns: string
      }
      v2_is_generated_template: {
        Args: { p_template_id: string }
        Returns: boolean
      }
      v2_is_workspace_member: {
        Args: { p_workspace_id: string }
        Returns: boolean
      }
      v2_is_workspace_owner: {
        Args: { p_workspace_id: string }
        Returns: boolean
      }
      v2_legacy_field_type: {
        Args: { p_field_key: string; p_question_type: string }
        Returns: string
      }
      v2_list_collection_previous_answer_pins: {
        Args: { p_field_id: string; p_session_id: string }
        Returns: {
          field_keys: string[]
          id: string
          logical_key: string
          pinned_at: string
          pinned_by: string | null
          row_order: Json
          sheet_key: string
          snapshot: Json
          source_key: string
          source_selector: Json | null
          source_title: string
          workspace_id: string
        }[]
        SetofOptions: {
          from: "*"
          to: "v2_collection_previous_answer_pins"
          isOneToOne: false
          isSetofReturn: true
        }
      }
      v2_list_currency_options: {
        Args: { p_workspace_id: string }
        Returns: {
          position: number
          symbol: string
        }[]
      }
      v2_lock_collection_scenario_session: {
        Args: { p_session_id: string }
        Returns: {
          created_at: string
          created_by: string
          creation_origin: string
          deleted_at: string | null
          deleted_by: string | null
          id: string
          last_edited_by: string | null
          status: string
          template_id: string | null
          title: string
          updated_at: string
          workspace_id: string
        }
        SetofOptions: {
          from: "*"
          to: "v2_collection_sessions"
          isOneToOne: true
          isSetofReturn: false
        }
      }
      v2_lock_collection_workspace: {
        Args: { p_session_id: string }
        Returns: undefined
      }
      v2_locked_collection_feedback_item: {
        Args: {
          p_field_id: string
          p_row_id: string
          p_session_id: string
          p_workspace_id: string
        }
        Returns: Json
      }
      v2_manual_payload_fingerprint: {
        Args: {
          p_conclusions: string
          p_next_entry_at: string
          p_notes: string
          p_occurred_at: string
          p_rows: Json
          p_source_kind: string
          p_workspace_id: string
        }
        Returns: string
      }
      v2_materialize_economic_schedule: {
        Args: { p_horizon_days?: number; p_schedule_id: string }
        Returns: number
      }
      v2_news_feed: {
        Args: { p_workspace_id: string }
        Returns: {
          body: string
          collection_session_id: string
          created_at: string
          created_by: string
          entity_id: string
          field_id: string
          id: string
          importance: string
          occurred_at: string
          origin: string
          project_id: string
          question_id: string
          raw_content: Json
          read_only: boolean
          source: Json
          source_id: string
          source_name: string
          trade_id: string
          updated_at: string
          updated_by: string
          worksheet_created_at: string
          workspace_id: string
        }[]
      }
      v2_news_learning_source: { Args: { p_url: string }; Returns: string }
      v2_news_pin_item_key: {
        Args: { p_id: string; p_origin: string; p_raw: Json }
        Returns: string
      }
      v2_news_source_identity: { Args: { p_source: string }; Returns: string }
      v2_normalize_structured_rows: {
        Args: { p_field_id: string; p_type: string; p_value: Json }
        Returns: Json
      }
      v2_open_collection: { Args: { p_session_id: string }; Returns: undefined }
      v2_patch_collection_rows_receipt: {
        Args: {
          p_field_id: string
          p_operation: string
          p_parent_row_id?: string
          p_patch?: Json
          p_row_id: string
          p_session_id: string
        }
        Returns: {
          created_at: string
          field_id: string
          id: string
          session_id: string
          updated_at: string
          updated_by: string | null
          updated_origin: string
          value: Json
        }[]
        SetofOptions: {
          from: "*"
          to: "v2_collection_values"
          isOneToOne: false
          isSetofReturn: true
        }
      }
      v2_pin_collection_previous_answer: {
        Args: {
          p_expected_row_order: Json
          p_expected_snapshot: Json
          p_field_id: string
          p_image_copies?: Json
          p_session_id: string
          p_source_field_id: string
          p_source_session_id: string
        }
        Returns: {
          field_keys: string[]
          id: string
          logical_key: string
          pinned_at: string
          pinned_by: string | null
          row_order: Json
          sheet_key: string
          snapshot: Json
          source_key: string
          source_selector: Json | null
          source_title: string
          workspace_id: string
        }
        SetofOptions: {
          from: "*"
          to: "v2_collection_previous_answer_pins"
          isOneToOne: true
          isSetofReturn: false
        }
      }
      v2_pin_collection_previous_news: {
        Args: {
          p_expected_row_order: Json
          p_expected_snapshot: Json
          p_field_id: string
          p_image_copies?: Json
          p_pin_id?: string
          p_selector: Json
          p_session_id: string
          p_source_field_id: string
          p_source_session_id: string
        }
        Returns: {
          field_keys: string[]
          id: string
          logical_key: string
          pinned_at: string
          pinned_by: string | null
          row_order: Json
          sheet_key: string
          snapshot: Json
          source_key: string
          source_selector: Json | null
          source_title: string
          workspace_id: string
        }
        SetofOptions: {
          from: "*"
          to: "v2_collection_previous_answer_pins"
          isOneToOne: true
          isSetofReturn: false
        }
      }
      v2_prepare_collection_previous_answer_pin: {
        Args: {
          p_field_id: string
          p_session_id: string
          p_source_field_id: string
          p_source_session_id: string
        }
        Returns: Json
      }
      v2_prepare_collection_previous_news_evidence: {
        Args: {
          p_field_id: string
          p_pin_id?: string
          p_role: string
          p_scenario_id: string
          p_selector: Json
          p_session_id: string
          p_source_field_id: string
          p_source_session_id: string
        }
        Returns: Json
      }
      v2_prepare_collection_previous_news_pin: {
        Args: {
          p_field_id: string
          p_pin_id?: string
          p_selector: Json
          p_session_id: string
          p_source_field_id: string
          p_source_session_id: string
        }
        Returns: Json
      }
      v2_prepare_collection_scenario_evidence: {
        Args: {
          p_field_id: string
          p_news_id: string
          p_role: string
          p_scenario_id: string
          p_session_id: string
        }
        Returns: Json
      }
      v2_purge_collection: {
        Args: { p_session_id: string }
        Returns: undefined
      }
      v2_purge_trade: { Args: { p_trade_id: string }; Returns: undefined }
      v2_rate_collection_news_feedback: {
        Args: {
          p_field_id: string
          p_row_id: string
          p_sentiment: string
          p_session_id: string
          p_workspace_id: string
        }
        Returns: undefined
      }
      v2_read_collection_news_feedback_origins: {
        Args: {
          p_field_id: string
          p_session_id: string
          p_workspace_id: string
        }
        Returns: {
          model_evaluation: Json
          row_id: string
        }[]
      }
      v2_read_collection_news_learning: {
        Args: {
          p_field_id: string
          p_source_url?: string
          p_workspace_id: string
        }
        Returns: Json
      }
      v2_read_collection_news_read_markers: {
        Args: {
          p_field_id: string
          p_session_id: string
          p_workspace_id: string
        }
        Returns: {
          is_current_user: boolean
          read_until: string
          updated_at: string
          user_id: string
        }[]
      }
      v2_read_collection_news_recycle_bin: {
        Args: {
          p_field_id?: string
          p_session_id?: string
          p_workspace_id: string
        }
        Returns: {
          body: string
          deleted_at: string
          deleted_by: string
          detail: string
          field_id: string
          id: string
          importance: string
          model_evaluation: Json
          reason: string
          restored_at: string
          restored_by: string
          row_id: string
          session_id: string
          source_name: string
          source_url: string
          workspace_id: string
        }[]
      }
      v2_read_collection_news_unread: {
        Args: {
          p_field_id: string
          p_observed_version: number
          p_session_id: string
        }
        Returns: Json
      }
      v2_read_news_pins: {
        Args: { p_session_id?: string; p_workspace_id: string }
        Returns: {
          body: string
          field_id: string
          importance: string
          item_id: string
          item_key: string
          logical_key: string
          origin_session_id: string
          pinned_at: string
          pinned_by: string
          row_id: string
          source_name: string
          updated_at: string
          updated_by: string
          value_id: string
        }[]
      }
      v2_register_collection_news_assessment: {
        Args: { p_item: Json; p_run_id: string }
        Returns: undefined
      }
      v2_release_ai_lease: { Args: { p_scope_key: string }; Returns: undefined }
      v2_remove_currency_option: {
        Args: { p_symbol: string; p_workspace_id: string }
        Returns: undefined
      }
      v2_restore_collection_news_feedback: {
        Args: { p_trash_id: string; p_workspace_id: string }
        Returns: undefined
      }
      v2_restore_trade: { Args: { p_trade_id: string }; Returns: undefined }
      v2_save_collection_scenario_role: {
        Args: {
          p_expected_html: string
          p_expected_value: Json
          p_role: string
          p_scenario_id: string
          p_session_id: string
          p_value: Json
        }
        Returns: Json
      }
      v2_save_collection_template_live: {
        Args: {
          p_active: boolean
          p_default: boolean
          p_expected_revision: number
          p_name: string
          p_sections: Json
          p_source_template_id: string
          p_template_id: string
          p_workspace_id: string
        }
        Returns: Json
      }
      v2_save_collection_value: {
        Args: { p_field_id: string; p_session_id: string; p_value: Json }
        Returns: undefined
      }
      v2_save_collection_value_receipt: {
        Args: {
          p_baseline?: Json
          p_field_id: string
          p_session_id: string
          p_value: Json
        }
        Returns: {
          created_at: string
          field_id: string
          id: string
          session_id: string
          updated_at: string
          updated_by: string | null
          updated_origin: string
          value: Json
        }[]
        SetofOptions: {
          from: "*"
          to: "v2_collection_values"
          isOneToOne: false
          isSetofReturn: true
        }
      }
      v2_save_economic_event: {
        Args: {
          p_event_at: string
          p_id: string
          p_importance: number
          p_name: string
          p_offsets: number[]
          p_workspace_id: string
        }
        Returns: string
      }
      v2_save_economic_notification_defaults: {
        Args: {
          p_important_offsets: number[]
          p_ordinary_offsets: number[]
          p_workspace_id: string
        }
        Returns: undefined
      }
      v2_save_economic_schedule: {
        Args: { p_offsets: number[]; p_schedule: Json }
        Returns: {
          active: boolean
          created_at: string
          created_by: string | null
          ends_on: string | null
          id: string
          importance: number
          market_calendar: string | null
          name: string
          reminder_offsets: number[]
          starts_on: string
          times: string[]
          timezone: string
          updated_at: string
          updated_by: string | null
          weekdays: number[]
          workspace_id: string
        }
        SetofOptions: {
          from: "*"
          to: "v2_economic_schedules"
          isOneToOne: true
          isSetofReturn: false
        }
      }
      v2_save_manual_entries: {
        Args: {
          p_batch_id: string
          p_conclusions: string
          p_fingerprint?: string
          p_next_entry_at?: string
          p_notes: string
          p_occurred_at: string
          p_rows: Json
          p_source_kind: string
          p_workspace_id: string
        }
        Returns: {
          archived_at: string | null
          archived_by: string | null
          batch_id: string
          break_even: boolean
          conclusions: string
          created_at: string
          created_by: string
          formula_name: string | null
          id: string
          notes: string
          occurred_at: string
          pnl: number | null
          row_key: string
          source_kind: string
          symbol: string
          updated_at: string
          updated_by: string | null
          workspace_id: string
        }[]
        SetofOptions: {
          from: "*"
          to: "v2_manual_entries"
          isOneToOne: false
          isSetofReturn: true
        }
      }
      v2_save_news_summary_preferences: {
        Args: { p_patch: Json; p_workspace_id: string }
        Returns: undefined
      }
      v2_save_template: {
        Args: {
          p_active: boolean
          p_default: boolean
          p_kind: string
          p_name: string
          p_sections: Json
          p_template_id: string
          p_workspace_id: string
        }
        Returns: string
      }
      v2_save_template_editor: {
        Args: {
          p_active: boolean
          p_default: boolean
          p_default_asset_symbol?: string
          p_kind: string
          p_name: string
          p_sections: Json
          p_source_template_id: string
          p_template_id: string
          p_workspace_id: string
        }
        Returns: string
      }
      v2_seed_default_templates: {
        Args: { p_workspace_id: string }
        Returns: undefined
      }
      v2_set_collection_news_order: {
        Args: {
          p_expected_revision: number
          p_field_id: string
          p_row_ids: string[]
          p_session_id: string
          p_workspace_id: string
        }
        Returns: Json
      }
      v2_set_collection_news_read_marker: {
        Args: {
          p_field_id: string
          p_read_until: string
          p_session_id: string
          p_workspace_id: string
        }
        Returns: undefined
      }
      v2_set_collection_trash: {
        Args: { p_deleted: boolean; p_session_id: string }
        Returns: undefined
      }
      v2_set_economic_event_type_importance: {
        Args: {
          p_important: boolean
          p_label: string
          p_type_key: string
          p_workspace_id: string
        }
        Returns: undefined
      }
      v2_set_economic_reminders: {
        Args: { p_event_id: string; p_offsets: number[]; p_schedule_id: string }
        Returns: undefined
      }
      v2_set_economic_type_reminders: {
        Args: {
          p_offsets?: number[]
          p_type_key: string
          p_workspace_id: string
        }
        Returns: undefined
      }
      v2_set_legacy_cycle_formula: {
        Args: { p_cycle_id: string; p_name: string; p_workspace_id: string }
        Returns: string
      }
      v2_set_manual_entry_archived: {
        Args: { p_archived: boolean; p_entry_id: string }
        Returns: undefined
      }
      v2_set_news_pin: {
        Args: { p_item_id: string; p_pinned: boolean; p_workspace_id: string }
        Returns: undefined
      }
      v2_set_report_archive: {
        Args: {
          p_archived: boolean
          p_source_key: string
          p_source_kind: string
          p_workspace_id: string
        }
        Returns: undefined
      }
      v2_set_trade_formula: {
        Args: { p_name: string; p_trade_id: string }
        Returns: string
      }
      v2_snooze_economic_reminder: {
        Args: { p_minutes?: number; p_reminder_id: string }
        Returns: string
      }
      v2_store_collection_news_feedback: {
        Args: {
          p_action: string
          p_detail: string
          p_item: Json
          p_level: string
          p_score: number
          p_sentiment: string
        }
        Returns: undefined
      }
      v2_sync_personal_event_reminders: {
        Args: { p_event_id: string }
        Returns: undefined
      }
      v2_template_in_use: { Args: { p_template_id: string }; Returns: boolean }
      v2_trade_button_progress: {
        Args: { p_field_id: string; p_trade_id: string }
        Returns: {
          completed: number
          completion_percent: number
          eligible: boolean
          required_percent: number
          total: number
        }[]
      }
      v2_trade_button_threshold: { Args: { p_settings: Json }; Returns: number }
      v2_trade_button_value: { Args: { p_value: Json }; Returns: Json }
      v2_trade_field_is_post_trade: {
        Args: {
          p_field_key: string
          p_field_settings: Json
          p_field_type: string
          p_section_settings: Json
          p_section_title: string
        }
        Returns: boolean
      }
      v2_unpin_collection_previous_answer: {
        Args: { p_field_id: string; p_pin_id: string; p_session_id: string }
        Returns: boolean
      }
      v2_update_news_summary_item: {
        Args: {
          p_expected: Json
          p_item_id: string
          p_patch: Json
          p_workspace_id: string
        }
        Returns: undefined
      }
      v2_us_equity_session: {
        Args: { p_market_date: string }
        Returns: {
          close_at: string
          open_at: string
        }[]
      }
      v2_valid_reminder_offsets: {
        Args: { p_offsets: number[] }
        Returns: number[]
      }
      v2_valid_timeframe_scenarios: {
        Args: { p_value: Json }
        Returns: boolean
      }
      v2_visible_trade_fields: {
        Args: { p_trade_id: string }
        Returns: string[]
      }
      v2_write_collection_feedback_rows: {
        Args: { p_item: Json; p_rows: Json }
        Returns: undefined
      }
    }
    Enums: {
      app_role: "admin" | "user"
    }
    CompositeTypes: {
      [_ in never]: never
    }
  }
}

type DatabaseWithoutInternals = Omit<Database, "__InternalSupabase">

type DefaultSchema = DatabaseWithoutInternals[Extract<keyof Database, "public">]

export type Tables<
  DefaultSchemaTableNameOrOptions extends
    | keyof (DefaultSchema["Tables"] & DefaultSchema["Views"])
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends (DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
        DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])
    : never) = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
      DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])[TableName] extends {
      Row: infer R
    }
    ? R
    : never
  : DefaultSchemaTableNameOrOptions extends keyof (DefaultSchema["Tables"] &
        DefaultSchema["Views"])
    ? (DefaultSchema["Tables"] &
        DefaultSchema["Views"])[DefaultSchemaTableNameOrOptions] extends {
        Row: infer R
      }
      ? R
      : never
    : never

export type TablesInsert<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends (DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never) = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Insert: infer I
    }
    ? I
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Insert: infer I
      }
      ? I
      : never
    : never

export type TablesUpdate<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends (DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never) = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Update: infer U
    }
    ? U
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Update: infer U
      }
      ? U
      : never
    : never

export type Enums<
  DefaultSchemaEnumNameOrOptions extends
    | keyof DefaultSchema["Enums"]
    | { schema: keyof DatabaseWithoutInternals },
  EnumName extends (DefaultSchemaEnumNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"]
    : never) = never,
> = DefaultSchemaEnumNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"][EnumName]
  : DefaultSchemaEnumNameOrOptions extends keyof DefaultSchema["Enums"]
    ? DefaultSchema["Enums"][DefaultSchemaEnumNameOrOptions]
    : never

export type CompositeTypes<
  PublicCompositeTypeNameOrOptions extends
    | keyof DefaultSchema["CompositeTypes"]
    | { schema: keyof DatabaseWithoutInternals },
  CompositeTypeName extends (PublicCompositeTypeNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"]
    : never) = never,
> = PublicCompositeTypeNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"][CompositeTypeName]
  : PublicCompositeTypeNameOrOptions extends keyof DefaultSchema["CompositeTypes"]
    ? DefaultSchema["CompositeTypes"][PublicCompositeTypeNameOrOptions]
    : never

export const Constants = {
  public: {
    Enums: {
      app_role: ["admin", "user"],
    },
  },
} as const
