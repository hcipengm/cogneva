import { create } from 'zustand';

export type MessageRole = 'user' | 'assistant' | 'system';

export interface Message {
  id: string;
  role: MessageRole;
  text: string;
  status: 'streaming' | 'done' | 'error';
}

export interface TaskProgress {
  taskId: string;
  progress: number;
  step: string;
}

export interface StreamState {
  messages: Message[];
  taskProgress: TaskProgress | null;
  connectionStatus: 'connecting' | 'open' | 'closed' | 'error';
  startMessage: (id: string, role: MessageRole) => void;
  appendDelta: (messageId: string, delta: string) => void;
  endMessage: (messageId: string) => void;
  setTaskProgress: (progress: TaskProgress) => void;
  setConnectionStatus: (status: StreamState['connectionStatus']) => void;
}

export const useStreamStore = create<StreamState>((set) => ({
  messages: [],
  taskProgress: null,
  connectionStatus: 'connecting',

  startMessage: (id, role) =>
    set((state) => ({
      messages: [
        ...state.messages,
        { id, role, text: '', status: 'streaming' },
      ],
    })),

  appendDelta: (messageId, delta) =>
    set((state) => ({
      messages: state.messages.map((message) =>
        message.id === messageId
          ? { ...message, text: message.text + delta }
          : message
      ),
    })),

  endMessage: (messageId) =>
    set((state) => ({
      messages: state.messages.map((message) =>
        message.id === messageId
          ? { ...message, status: 'done' }
          : message
      ),
    })),

  setTaskProgress: (progress) => set({ taskProgress: progress }),

  setConnectionStatus: (status) => set({ connectionStatus: status }),
}));
